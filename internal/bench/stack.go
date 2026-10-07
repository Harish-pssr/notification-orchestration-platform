package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Stack controls the Docker Compose stack under test: scaling the relay and consumers,
// stopping and starting services, and running the load generator inside the Compose
// network so Docker Desktop's host port forwarding is never part of a measurement (D13).
type Stack struct {
	ComposeFiles []string // e.g. docker-compose.yml, docker-compose.dev.yml
	Dir          string   // project directory
	APIURL       string   // API as seen from the host, for health checks
}

func (s Stack) compose(ctx context.Context, args ...string) *exec.Cmd {
	full := []string{"compose"}
	for _, f := range s.ComposeFiles {
		full = append(full, "-f", f)
	}
	full = append(full, args...)
	cmd := exec.CommandContext(ctx, "docker", full...)
	cmd.Dir = s.Dir
	return cmd
}

func (s Stack) run(ctx context.Context, args ...string) (string, error) {
	var out bytes.Buffer
	cmd := s.compose(ctx, args...)
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

// Consumers are the three consumer services, scaled together.
var consumerServices = []string{"consumer-email", "consumer-push", "consumer-webhook"}

// Scale sets the number of relay instances and of consumer instances per channel.
func (s Stack) Scale(ctx context.Context, relays, consumers int) error {
	args := []string{"up", "-d", "--no-deps", "--scale", "relay=" + strconv.Itoa(relays)}
	for _, c := range consumerServices {
		args = append(args, "--scale", c+"="+strconv.Itoa(consumers))
	}
	args = append(args, "relay")
	args = append(args, consumerServices...)
	if out, err := s.run(ctx, args...); err != nil {
		return fmt.Errorf("scale: %w\n%s", err, out)
	}
	return nil
}

// Stop and Start control one service (a real outage, not a simulated one).
func (s Stack) Stop(ctx context.Context, service string) error {
	if out, err := s.run(ctx, "stop", service); err != nil {
		return fmt.Errorf("stop %s: %w\n%s", service, err, out)
	}
	return nil
}

func (s Stack) Start(ctx context.Context, service string) error {
	if out, err := s.run(ctx, "start", service); err != nil {
		return fmt.Errorf("start %s: %w\n%s", service, err, out)
	}
	return nil
}

// roleLive is the live instance count per role from GET /api/health.
func (s Stack) roleLive(ctx context.Context) (map[string]int, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.APIURL+"/api/health", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		Roles []struct {
			Name string `json:"name"`
			Live int    `json:"live"`
		} `json:"roles"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	live := map[string]int{}
	for _, r := range body.Roles {
		live[r.Name] = r.Live
	}
	return live, nil
}

// WaitReady blocks until exactly the wanted number of live instances report a fresh
// heartbeat for the relay and for each consumer role.
func (s Stack) WaitReady(ctx context.Context, relays, consumers int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		live, err := s.roleLive(ctx)
		if err == nil && live["relay"] == relays &&
			live["consumer-email"] == consumers && live["consumer-push"] == consumers && live["consumer-webhook"] == consumers {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("stack not ready after %s (live=%v, want relay=%d, consumers=%d each; last error: %v)", timeout, live, relays, consumers, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// RunLoad runs the open-loop generator inside the Compose network and returns its summary.
func (s Stack) RunLoad(ctx context.Context, cfg OpenLoopConfig) (RunSummary, error) {
	args := []string{"--profile", "bench", "run", "--rm", "-T", "loadgen", "run",
		"-url", "http://api:8080",
		"-rate", strconv.FormatFloat(cfg.Rate, 'f', -1, 64),
		"-duration", cfg.Duration.String(),
		"-run", cfg.Run, "-json"}
	if cfg.MaxInflight > 0 {
		args = append(args, "-max-inflight", strconv.Itoa(cfg.MaxInflight))
	}
	out, err := s.run(ctx, args...)
	if err != nil {
		return RunSummary{}, fmt.Errorf("load generator failed: %w\n%s", err, out)
	}
	// The container may print other lines (image build, network); the summary is the JSON line.
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "{") {
			var sum RunSummary
			if err := json.Unmarshal([]byte(line), &sum); err == nil {
				return sum, nil
			}
		}
	}
	return RunSummary{}, fmt.Errorf("no summary in load generator output:\n%s", out)
}

// CPUByService samples the CPU use of every running container once (docker stats) and
// sums it per Compose service. 100 means one full core; relay and consumer replicas are
// added together. It takes about two seconds, so callers sample it on a timer.
func (s Stack) CPUByService(ctx context.Context) (map[string]float64, error) {
	cmd := exec.CommandContext(ctx, "docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}}")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	totals := map[string]float64{}
	for _, line := range strings.Split(out.String(), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		pct, err := strconv.ParseFloat(strings.TrimSuffix(f[1], "%"), 64)
		if err != nil {
			continue
		}
		totals[serviceOf(f[0])] += pct
	}
	return totals, nil
}

// serviceOf turns a container name such as "notifplatform-consumer-email-3" into its
// Compose service, "consumer-email".
func serviceOf(container string) string {
	name := strings.TrimPrefix(container, "notifplatform-")
	if i := strings.LastIndex(name, "-"); i > 0 {
		if _, err := strconv.Atoi(name[i+1:]); err == nil {
			return name[:i]
		}
	}
	return name
}

// ContainerNames lists the running containers of one Compose service.
func (s Stack) ContainerNames(ctx context.Context, service string) ([]string, error) {
	cmd := exec.CommandContext(ctx, "docker", "ps", "--filter", "label=com.docker.compose.service="+service,
		"--filter", "status=running", "--format", "{{.Names}}")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("list %s containers: %w", service, err)
	}
	return strings.Fields(out.String()), nil
}

// Kill sends SIGKILL to one container: a real crash, with no chance to clean up.
func (s Stack) Kill(ctx context.Context, container string) error {
	if out, err := exec.CommandContext(ctx, "docker", "kill", container).CombinedOutput(); err != nil {
		return fmt.Errorf("kill %s: %w\n%s", container, err, out)
	}
	return nil
}
