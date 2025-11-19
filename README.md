# Code Sample to Get On-Chain Data from Bitquery Kafka Streams in Protobuf format

Read more on Bitquery Kafka Stream [here](https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/)

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Credentials

Edit `config.py` and add your Bitquery Kafka credentials:
```python
username = 'your_bitquery_username'
password = 'your_bitquery_password'
```

### 3. Protobuf Messages

The code uses the `bitquery-pb2-kafka-package` package (already in requirements.txt) which provides all necessary protobuf message types. The package includes:

**For Solana:**
- `block_message_pb2.BlockMessage`
- `dex_block_message_pb2.DexBlockMessage`
- `ohlc_message_pb2.OhlcMessage`
- `parsed_idl_block_message_pb2.ParsedIdlBlockMessage`
- `token_block_message_pb2.TokenBlockMessage` ✅

**Note:** The code supports both:
- `solana.tokens.proto` topic → uses `TokenBlockMessage`
- `solana.transactions.proto` topic → uses `ParsedIdlBlockMessage`

See the [package documentation](https://pypi.org/project/bitquery-pb2-kafka-package/) for more details.

### 4. Run the Consumer

```bash
python consumer.py
```

The consumer will:
- Connect to Bitquery Kafka streams
- Subscribe to `solana.tokens.proto` topic (or `solana.transactions.proto` if changed)
- Write block information (slot number and timestamp) to `blocks.log` file
- Print only errors and final statistics to terminal
- Process messages until interrupted (Ctrl+C)
- On shutdown, report any missing blocks in the sequence

**Output:**
- **Terminal**: Only errors and final statistics report
- **File**: All blocks written to `blocks.log` (or custom file via `BLOCKS_FILE` environment variable)

Example:
```bash
# Use default blocks.log file
python consumer.py

# Use custom output file
BLOCKS_FILE=my_blocks.log python consumer.py
```

## Notes

- The consumer uses a unique group ID to avoid conflicts
- Messages are processed from the latest offset by default
- Press Ctrl+C to gracefully shutdown the consumer
