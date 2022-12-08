python3 -m grpc_tools.protoc -I./grpc --python_out=./master_node/protocol --mypy_out=./master_node/protocol --grpc_python_out=./master_node/protocol protocol.proto
code1=$?
python3 -m grpc_tools.protoc -I./grpc --python_out=./worker_node/protocol --mypy_out=./worker_node/protocol --grpc_python_out=./worker_node/protocol protocol.proto
code2=$?

if [ $code1 != 0 ] || [ $code2 != 0 ]
then
    exit 1
fi

#quick fix for the import
sed -i 's/import protocol_pb2/from protocol import protocol_pb2/g' ./master_node/protocol/protocol_pb2_grpc.py
code1=$?
sed -i 's/import protocol_pb2/from protocol import protocol_pb2/g' ./worker_node/protocol/protocol_pb2_grpc.py
code2=$?

if [ $code1 != 0 ] || [ $code2 != 0 ]
then
    exit 1
else
    exit 0
fi