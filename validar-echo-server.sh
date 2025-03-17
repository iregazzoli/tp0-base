NETWORK_NAME="tp0_testing_net"
SERVER_CONTAINER="server"  
SERVER_PORT=12345
TEST_MESSAGE="Hello, World!"

if [ ! "$(docker ps -q -f name=$SERVER_CONTAINER)" ]; then
    echo "Container not running."
    exit 1
fi

echo "Sending message..."
RESPONSE=$(echo "$TEST_MESSAGE" | docker run --pull never --rm --network "$NETWORK_NAME" -i client:latest sh -c "nc $SERVER_CONTAINER $SERVER_PORT"
)
echo "Received response: $RESPONSE"

if [ "$RESPONSE" == "$TEST_MESSAGE" ]; then
    printf "\033[32maction: test_echo_server | result: success\033[0m\n"
else
    printf "\033[31maction: test_echo_server | result: fail\033[0m\n"
fi


