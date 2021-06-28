rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*
cd ../../mrapps && go build -race -buildmode=plugin wc.go

cd .. 
go build -race mrcoordinator.go
go build -race mrworker.go
go build -race mrsequential.go

mrsequential ../mrapps/wc.so pg*txt