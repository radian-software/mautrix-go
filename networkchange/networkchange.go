package networkchange

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"

	"maunium.net/go/mautrix/exsync"
)

var errNetworkChange error = errors.New("network change")

func DoRequest(client *http.Client, networkChange *exsync.Event, req *http.Request) (*http.Response, error) {
	if networkChange == nil {
		return client.Do(req)
	}
	if req.Body != nil {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	for {
		ctx, cancel := context.WithCancelCause(req.Context())
		go func() {
			networkChange.Wait(ctx)
			cancel(errNetworkChange)
		}()
		resp, err := client.Do(req.WithContext(ctx))
		if errors.Is(err, errNetworkChange) {
			trans := client.Transport
			if trans == nil {
				trans = http.DefaultTransport
			}
			client.Transport = trans.(*http.Transport).Clone()
			continue
		}
		return resp, err
	}
}
