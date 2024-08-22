import threading
import time
from azure.eventhub import EventHubConsumerClient

# Replace with your Event Hub connection string
connection_str = ''
eventhub_name = ''  # Name of your Event Hub
consumer_group = ''  # Replace with your consumer group if different

def on_event(partition_context, event):
    print(f"Received event from partition: {partition_context.partition_id}")
    print(f"Event data: {event.body_as_str()}")
    partition_context.update_checkpoint(event)

def on_error(partition_context, error):
    print(f"An error occurred on partition: {partition_context.partition_id}. Error: {error}")

def on_partition_initialize(partition_context):
    print(f"Partition: {partition_context.partition_id} has been initialized.")

def on_partition_close(partition_context, reason):
    print(f"Partition: {partition_context.partition_id} has been closed. Reason: {reason}")

# Client instantiation with eventhub_name included
client = EventHubConsumerClient.from_connection_string(
    connection_str, consumer_group=consumer_group, eventhub_name=eventhub_name
)

def receive_events():
    with client:
        client.receive(
            on_event=on_event,
            on_error=on_error,
            on_partition_initialize=on_partition_initialize,
            on_partition_close=on_partition_close,
            starting_position="-1",  # "-1" means starting from the beginning of the stream
        )

# Create a thread for the event receiver
receive_thread = threading.Thread(target=receive_events)

# Start the thread
receive_thread.start()

# Set the timeout duration (2 minutes in this case)
timeout_duration = 120  # 120 seconds = 2 minutes

# Wait for the thread to finish or timeout
receive_thread.join(timeout=timeout_duration)

# After the timeout, if the thread is still running, gracefully shut it down
if receive_thread.is_alive():
    print("Timeout reached, stopping the client.")
    client.close()
    receive_thread.join()

print("Listening has stopped.")
