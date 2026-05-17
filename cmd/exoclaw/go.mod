module github.com/standd/exoclaw-go/cmd/exoclaw

go 1.25.0

require (
	github.com/standd/exoclaw-go v0.0.0
	github.com/standd/exoclaw-go/channels/stdin v0.0.0
	github.com/standd/exoclaw-go/conversation-file v0.0.0
	github.com/standd/exoclaw-go/providers/openai v0.0.0
	github.com/standd/exoclaw-go/tools/cron v0.0.0
	github.com/standd/exoclaw-go/tools/workspace v0.0.0
)

replace github.com/standd/exoclaw-go => ../../

replace github.com/standd/exoclaw-go/channels/stdin => ../../channels/stdin

replace github.com/standd/exoclaw-go/conversation-file => ../../conversation-file

replace github.com/standd/exoclaw-go/providers/openai => ../../providers/openai

replace github.com/standd/exoclaw-go/tools/workspace => ../../tools/workspace

replace github.com/standd/exoclaw-go/tools/cron => ../../tools/cron
