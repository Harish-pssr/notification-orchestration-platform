package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// EmailMustBeOff refuses to start a benchmark while the provider is sending REAL emails (D21): a load run would
// try to send one real email per event. An API that does not know the endpoint (an older build) or that cannot
// be reached is not treated as an error here; the benchmark fails on its own in that case.
func EmailMustBeOff(ctx context.Context, apiURL string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/api/provider/email", nil)
	if err != nil {
		return nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var st struct {
		Real bool `json:"real"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return nil
	}
	if st.Real {
		return fmt.Errorf("real email sending is switched ON: a benchmark would try to send a real email per event. " +
			"Switch it off on the Duplicates page (Email delivery) and run the benchmark again")
	}
	return nil
}
