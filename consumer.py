# This code displays latest transactions on Solana
# It can be reused for all topics, all chains by simply changing the topic, username, password and the Proto file import.

import uuid
import base58
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from google.protobuf.descriptor import FieldDescriptor
from solana import parsed_idl_block_message_pb2
from solana import token_block_message_pb2
import logging
import os
import config
import datetime
import threading
import signal

# Kafka consumer configuration
group_id_suffix = uuid.uuid4().hex
conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092',
    'group.id': f'{config.username}-group-{group_id_suffix}',  
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_PLAINTEXT',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': config.username,
    'sasl.password': config.password,
    'auto.offset.reset': 'latest',
}

consumer = Consumer(conf)
topic = 'solana.tokens.proto' 
consumer.subscribe([topic])

# Control flag for graceful shutdown
shutdown_event = threading.Event()
processed_count = 0
decode_error_count = 0
# Track all received block slots (using set to handle duplicates and out-of-order)
received_blocks = set()
start_time = None

# Output file for blocks (default: blocks.log, can be overridden with BLOCKS_FILE env var)
blocks_file_path = os.environ.get('BLOCKS_FILE', 'blocks.log')
blocks_file = None

# Output file for stats (default: stats.log, can be overridden with STATS_FILE env var)
stats_file_path = os.environ.get('STATS_FILE', 'stats.log')
stats_file = None


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---  recursive traversal and print --- #

def convert_bytes(value, encoding='base58'):
    if encoding == 'base58':
        return base58.b58encode(value).decode()
    return value.hex()

def print_protobuf_message(msg, indent=0, encoding='base58'):
    prefix = ' ' * indent
    for field in msg.DESCRIPTOR.fields:
        value = getattr(msg, field.name)

        if field.label == FieldDescriptor.LABEL_REPEATED: # The field is a repeated (i.e. array/list) field.
            if not value:
                continue
            print(f"{prefix}{field.name} (repeated):")
            for idx, item in enumerate(value):
                if field.type == FieldDescriptor.TYPE_MESSAGE: # The field is a nested protobuf message.
                    print(f"{prefix}  [{idx}]:")
                    print_protobuf_message(item, indent + 4, encoding)
                elif field.type == FieldDescriptor.TYPE_BYTES:
                    print(f"{prefix}  [{idx}]: {convert_bytes(item, encoding)}")
                else:
                    print(f"{prefix}  [{idx}]: {item}")

        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            if msg.HasField(field.name):
                print(f"{prefix}{field.name}:")
                print_protobuf_message(value, indent + 4, encoding)

        elif field.type == FieldDescriptor.TYPE_BYTES:
            print(f"{prefix}{field.name}: {convert_bytes(value, encoding)}")

        elif field.containing_oneof:
            if msg.WhichOneof(field.containing_oneof.name) == field.name:
                print(f"{prefix}{field.name} (oneof): {value}")

        else:
            print(f"{prefix}{field.name}: {value}")

def process_message(buffer):
    """Process a single protobuf message"""
    global received_blocks, start_time, blocks_file
    try:
        # Try TokenBlockMessage first (for solana.tokens.proto topic)
        try:
            token_block = token_block_message_pb2.TokenBlockMessage()
            token_block.ParseFromString(buffer)
            
            timestamp = datetime.datetime.now(datetime.timezone.utc)
            block_slot = token_block.Header.Slot
            
            # Track the block slot
            received_blocks.add(block_slot)
            
            # Record start time on first message
            if start_time is None:
                start_time = timestamp

            # Write to file instead of printing
            if blocks_file:
                blocks_file.write(f"Block: {block_slot} | Time: {timestamp}\n")
                blocks_file.flush()  # Ensure immediate write
            return
        except (DecodeError, AttributeError):
            # Fall back to ParsedIdlBlockMessage (for solana.transactions.proto topic)
            pass
        
        # Try ParsedIdlBlockMessage
        tx_block = parsed_idl_block_message_pb2.ParsedIdlBlockMessage()
        tx_block.ParseFromString(buffer)

        timestamp = datetime.datetime.now(datetime.timezone.utc)
        block_slot = tx_block.Header.Slot
        
        # Track the block slot
        received_blocks.add(block_slot)
        
        # Record start time on first message
        if start_time is None:
            start_time = timestamp

        # Write to file instead of printing
        if blocks_file:
            blocks_file.write(f"Block: {block_slot} | Time: {timestamp}\n")
            blocks_file.flush()  # Ensure immediate write

        # below code will print tx signature and block number, uncommment if you need to test
        #    if hasattr(tx_block, 'Transactions') and tx_block.Transactions:
        #        tx_signature = tx_block.Transactions[0].Signature
  
        #        signature_str = base58.b58encode(tx_signature).decode()
        #        print(f"\n Transaction: {signature_str} | Block: {tx_block.Header.Slot} | Time: {timestamp}")
        #    else:
        #        print(f"\n Block: {tx_block.Header.Slot} | Time: {timestamp}")
                
        # print_protobuf_message(tx_block, encoding='base58') # uncomment this to print the message

    except DecodeError as err:
        global decode_error_count
        decode_error_count += 1
        # Only log every 10th error to reduce noise, but always log first few
        if decode_error_count <= 3 or decode_error_count % 10 == 0:
            buffer_size = len(buffer) if buffer else 0
            logger.warning(f"Protobuf decoding error (count: {decode_error_count}, buffer size: {buffer_size} bytes): {err}")
    except Exception as err:
        logger.error(f"Error processing message: {err}")

def analyze_missing_blocks():
    """Analyze received blocks and identify missing blocks in the sequence"""
    global stats_file
    
    if not received_blocks:
        msg = "No blocks received during this session."
        logger.info(msg)
        if stats_file:
            stats_file.write(msg + "\n")
        return
    
    sorted_blocks = sorted(received_blocks)
    min_block = sorted_blocks[0]
    max_block = sorted_blocks[-1]
    total_blocks_in_range = max_block - min_block + 1
    received_count = len(received_blocks)
    
    # Helper function to write to both logger and stats file
    def write_stat(msg, level='info'):
        if level == 'info':
            logger.info(msg)
        elif level == 'warning':
            logger.warning(msg)
        if stats_file:
            stats_file.write(msg + "\n")
            stats_file.flush()
    
    write_stat("=" * 80)
    write_stat("BLOCK ANALYSIS REPORT")
    write_stat("=" * 80)
    write_stat(f"Session Duration: {start_time} to {datetime.datetime.now(datetime.timezone.utc)}")
    write_stat(f"Total Messages Processed: {processed_count}")
    error_percentage = (decode_error_count/processed_count*100) if processed_count > 0 else 0
    write_stat(f"Protobuf Decode Errors: {decode_error_count} ({error_percentage:.1f}% of messages)")
    write_stat(f"Unique Blocks Received: {received_count}")
    write_stat(f"Block Range: {min_block} to {max_block}")
    write_stat(f"Expected Blocks in Range: {total_blocks_in_range}")
    write_stat(f"Missing Blocks: {total_blocks_in_range - received_count}")
    write_stat("")
    
    # Find missing blocks
    expected_blocks = set(range(min_block, max_block + 1))
    missing_blocks = sorted(expected_blocks - received_blocks)
    
    if missing_blocks:
        write_stat(f"Found {len(missing_blocks)} missing block(s):", 'warning')
        
        # Group consecutive missing blocks for cleaner output
        if len(missing_blocks) <= 50:
            # Show all if not too many
            for block in missing_blocks:
                write_stat(f"  Missing Block: {block}", 'warning')
        else:
            # Show first 20 and last 20, with summary
            write_stat("  First 20 missing blocks:", 'warning')
            for block in missing_blocks[:20]:
                write_stat(f"    {block}", 'warning')
            write_stat(f"  ... ({len(missing_blocks) - 40} more blocks) ...", 'warning')
            write_stat("  Last 20 missing blocks:", 'warning')
            for block in missing_blocks[-20:]:
                write_stat(f"    {block}", 'warning')
        
        # Find consecutive ranges of missing blocks
        if len(missing_blocks) > 1:
            write_stat("")
            write_stat("Missing Block Ranges:")
            ranges = []
            start = missing_blocks[0]
            end = missing_blocks[0]
            
            for i in range(1, len(missing_blocks)):
                if missing_blocks[i] == end + 1:
                    end = missing_blocks[i]
                else:
                    if start == end:
                        ranges.append(f"{start}")
                    else:
                        ranges.append(f"{start}-{end}")
                    start = missing_blocks[i]
                    end = missing_blocks[i]
            
            # Add the last range
            if start == end:
                ranges.append(f"{start}")
            else:
                ranges.append(f"{start}-{end}")
            
            write_stat(f"  {', '.join(ranges)}")
    else:
        write_stat("✓ No missing blocks detected! All blocks in the range were received.")
    
    write_stat("=" * 80)

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()

# --- Main execution --- #

def main():
    global processed_count, blocks_file, stats_file
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Open blocks output file
    try:
        blocks_file = open(blocks_file_path, 'a')  # Append mode
        logger.info(f"Writing blocks to: {blocks_file_path}")
    except Exception as e:
        logger.error(f"Failed to open blocks file {blocks_file_path}: {e}")
        logger.info("Blocks will not be logged to file")
        blocks_file = None
    
    # Open stats output file
    try:
        stats_file = open(stats_file_path, 'w')  # Write mode (overwrite)
        logger.info(f"Writing stats to: {stats_file_path}")
    except Exception as e:
        logger.error(f"Failed to open stats file {stats_file_path}: {e}")
        logger.info("Stats will not be logged to file")
        stats_file = None
    
    # Main thread: Kafka polling loop
    try:
        while not shutdown_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            try:
                process_message(msg.value())
                processed_count += 1
            except Exception as err:
                logger.exception(f"Failed to process message: {err}")
               
                
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception(f"Error in main polling loop: {e}")
    finally:
        # Graceful shutdown
        logger.info("Initiating graceful shutdown...")
        shutdown_event.set()

        # Analyze missing blocks before closing
        analyze_missing_blocks()

        # Close blocks file
        if blocks_file:
            blocks_file.close()
            logger.info(f"Blocks log saved to: {blocks_file_path}")

        # Close stats file
        if stats_file:
            stats_file.close()
            logger.info(f"Stats report saved to: {stats_file_path}")

        # Close Kafka consumer
        consumer.close()
        logger.info(f"Shutdown complete. Total messages processed: {processed_count}")

if __name__ == "__main__":
    main()
