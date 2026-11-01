// 建立 MCP server，呼叫工具註冊函式
package mcp

import (
	"net/http"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func NewServer() *mcpsdk.Server {
	return mcpsdk.NewServer(
		&mcpsdk.Implementation{
			Name:    "jetrescue",
			Version: "0.1.0",
		},
		nil,
	)
}

func NewHTTPHandler(server *mcpsdk.Server) http.Handler {
	return mcpsdk.NewStreamableHTTPHandler(
		func(_ *http.Request) *mcpsdk.Server {
			return server
		},
		nil,
	)
}
