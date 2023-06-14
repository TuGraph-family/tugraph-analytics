// protoc --java_out=./ --proto_path=geaflow-state/geaflow-state-portal/src/main/resources/protobuf state_info.pb
syntax = "proto3";

option java_package = "com.antgroup.geaflow.view.meta";

message ViewMeta
{
  map<string, bytes> kvInfo = 1;
}