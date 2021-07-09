MIT 6.824 Distributed Systems Labs

To get mrworker running in the vscode debugger plugins must be built with: -gcflags='all=-N -l'
```go build -race -buildmode=plugin -gcflags='all=-N -l' wc.go```

To handle timeout command not working on Mac:
```brew install coreutils```