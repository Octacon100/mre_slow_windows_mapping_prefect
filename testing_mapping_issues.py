import time
import asyncio
from prefect import flow, task, get_run_logger
from prefect.client.orchestration import get_client


async def wait_aggressive(futures):
    """Poll task states more aggressively"""
    while True:
        # Check if all futures are completed
        if all(f.state.is_final() for f in futures):
            break
        await asyncio.sleep(0.1)  # Poll every 100ms


@task
def dummy_task(x):
    return x


@flow
def test_mapping():
    logger = get_run_logger()
    start = time.time()
    futures = dummy_task.map(range(100))
    map_time = time.time() - start
    logger.info(f"Mapping took: {map_time:.2f}s")
    
    start = time.time()
    # Use aggressive polling instead of futures.wait()
    asyncio.run(wait_aggressive(futures))
    wait_time = time.time() - start
    logger.info(f"Waiting took: {wait_time:.2f}s")

    logger.info(f"Final Notes:")
    logger.info(f"Mapping took: {map_time:.2f}s")
    logger.info(f"Waiting took: {wait_time:.2f}s")


@flow
def test_submitting():
    logger = get_run_logger()
    start = time.time()
    
    # Submit tasks one at a time instead of map()
    futures = []
    for i in range(100):
        future = dummy_task.submit(i)
        future.wait()
        #futures.append(future)
    
    submit_and_wait_time = time.time() - start
    logger.info(f"Submitting and waiting took: {submit_and_wait_time:.2f}s")
    
    start = time.time()
    # Wait for all to complete
    # results = [f.wait() for f in futures]
    # wait_time = time.time() - start
    # logger.info(f"Waiting took: {wait_time:.2f}s")

    # logger.info(f"Final Notes:")
    # logger.info(f"Submitting took: {map_time:.2f}s")
    # logger.info(f"Waiting took: {wait_time:.2f}s")


if __name__ == "__main__":
    test_mapping()
    # test_submitting()