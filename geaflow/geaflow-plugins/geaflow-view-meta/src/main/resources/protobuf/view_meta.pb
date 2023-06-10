// protoc --java_out=./ --proto_path=gryphon-state/gryphon-state-portal/src/main/resources/protobuf state_info.pb
syntax = "proto3";

option java_package = "com.antgroup.gryphon.view.meta";

message ViewMeta
{
  map<string, bytes> kvInfo = 1;
}