import asyncio
from src.output_adapters.http_adapter import setup_druid_kafka_ingestion

async def main():
    print("Connecting Kafka topics to Apache Druid...")
    print("Connecting to Druid Router at http://localhost:8888")
    print("-" * 50)
    
    # Creates Kafka supervisors linking your topics to Druid datasources
    results = await setup_druid_kafka_ingestion(
        druid_router_url='http://localhost:8888',
        kafka_brokers='kafka:29092' # 'kafka' is the Docker network hostname
    )
    
    for event_type, success in results.items():
        status = '✓ Created' if success else '✗ Failed'
        print(f"  {event_type}: {status}")
        
    print("-" * 50)
    print("Done! Check the Druid console or query your datasources.")

if __name__ == '__main__':
    asyncio.run(main())