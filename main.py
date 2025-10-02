import logging
import time
import os
import threading
from contextlib import contextmanager


from config.redis_pubsub import get_pubsub_manager, RedisPubSubManager
from services.message_consumer import MessageConsumerService


logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


@contextmanager
def worker_lifespan():
    logger.info("🚀 Starting Worker Service lifespan...")
    
    consumer_instance = None
    pubsub_manager_instance = None
    
    try:
        # 1. Khởi tạo và kiểm tra kết nối Redis
        pubsub_manager_instance = get_pubsub_manager()
        if not pubsub_manager_instance.health_check():
            raise RuntimeError("❌ Failed to connect to Redis. Exiting.")
        logger.info("✅ Redis connection established.")

        # 2. Khởi tạo và khởi động MessageConsumerService
        consumer_instance = MessageConsumerService.get_instance()
        consumer_instance.start()
        logger.info("✅ MessageConsumerService started.")
        
        logger.info("✅ Worker Service is ready and listening for messages!")
        yield # Worker sẽ chạy ở đây
    
    except Exception as e:
        logger.critical(f"❌ Worker Service startup critical error: {e}", exc_info=True)
        # Trong trường hợp lỗi nghiêm trọng khi khởi động, chúng ta thoát
        # hoặc báo hiệu cho hệ thống quản lý tiến trình.
        # Ở đây, chúng ta sẽ để nó re-raise và chương trình sẽ thoát.
        raise
    
    finally:
        # SHUTDOWN (dù có lỗi startup hay không)
        logger.info("🛑 Shutting down Worker Service lifespan...")
        if consumer_instance:
            try:
                consumer_instance.stop()
                logger.info("✅ MessageConsumerService stopped.")
            except Exception as e:
                logger.error(f"❌ Error stopping MessageConsumerService: {e}", exc_info=True)
        
        if pubsub_manager_instance:
            try:
                pubsub_manager_instance.close()
                logger.info("✅ Redis connections closed.")
            except Exception as e:
                logger.error(f"❌ Error closing Redis connections: {e}", exc_info=True)
        
        logger.info("✅ Worker Service shutdown complete.")


if __name__ == "__main__":
    logger.info("Starting Worker Service as a standalone process.")
    
    with worker_lifespan():
        # Worker sẽ chạy ở đây.
        # Để giữ cho chương trình chạy vô thời hạn (như một dịch vụ daemon),
        # chúng ta cần một vòng lặp hoặc chờ đợi một sự kiện dừng.
        # MessageConsumerService đã chạy trên một luồng riêng (consumer_thread),
        # nên luồng chính chỉ cần chờ cho đến khi có tín hiệu dừng.
        # Tuy nhiên, nếu luồng chính thoát, các luồng daemon cũng sẽ bị kill.
        # Do đó, chúng ta cần một cơ chế chờ.

        # Một cách đơn giản là block luồng chính cho đến khi Ctrl+C (KeyboardInterrupt)
        # hoặc một tín hiệu dừng được nhận.
        try:
            while True:
                time.sleep(1) # Ngủ luồng chính để tránh ngốn CPU, nhưng vẫn cho phép thoát
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Initiating graceful shutdown.")
        except Exception as e:
            logger.error(f"Unhandled exception in main worker loop: {e}", exc_info=True)

    logger.info("Worker Service process exited.")