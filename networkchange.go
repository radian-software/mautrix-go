package mautrix

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/exsync"
)

var errEscapedContext error = errors.New("escaped context, this is a programming error")
var errNetworkChange error = errors.New("network change")

func (cli *Client) doRequest(req *http.Request) (*http.Response, error) {
	if cli.NetworkChangeEvent == nil {
		return cli.Client.Do(req)
	}
	cli.networkChangeEventLock.Lock()
	if !cli.networkChangeEventInitDone {
		cli.resetPendingRequestsEvent = exsync.NewEvent()
		go func() {
			for {
				cli.NetworkChangeEvent.Wait(context.Background())
				cli.Log.Debug().Msg("Replacing HTTP transport due to network change")
				trans := cli.Client.Transport
				if trans == nil {
					trans = http.DefaultTransport
				}
				cli.Client.Transport = trans.(*http.Transport).Clone()
				cli.resetPendingRequestsEvent.Notify()
			}
		}()
		cli.networkChangeEventInitDone = true
	}
	cli.networkChangeEventLock.Unlock()
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
			cli.resetPendingRequestsEvent.Wait(ctx)
			if ctx.Err() == nil {
				cancel(errNetworkChange)
			}
		}()
		startTime := time.Now()
		resp, err := cli.Client.Do(req.WithContext(ctx))
		duration := time.Since(startTime)
		if context.Cause(ctx) == errNetworkChange {
			zerolog.Ctx(ctx).Debug().
				Str("method", req.Method).
				Str("url", req.URL.String()).
				Dur("duration", duration).
				Msg("Retrying request immediately due to network change")
			continue
		}
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			if readErr == nil {
				resp.Body = io.NopCloser(bytes.NewReader(body))
			} else {
				err = readErr
			}
		}
		cancel(errEscapedContext)
		return resp, err
	}
}
