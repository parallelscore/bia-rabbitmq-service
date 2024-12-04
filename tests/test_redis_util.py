# import re
# import pytest
# from unittest.mock import AsyncMock, patch, MagicMock
# from aioredis import RedisError
# from app.utils.redis_util import RedisUtil  # Ensure path matches your module structure


# @pytest.fixture
# def redis_util():
#     """Fixture to initialize RedisUtil instance."""
#     return RedisUtil()


# @pytest.mark.asyncio
# async def test_connect_success(redis_util):
#     with patch("app.utils.redis_util.aioredis.from_url", new_callable=AsyncMock) as mock_redis, \
#          patch.object(redis_util, "logger") as mock_logger:

#         mock_redis_instance = AsyncMock()
#         mock_redis.return_value = mock_redis_instance

#         await redis_util.connect()

#         assert mock_redis.called, "aioredis.from_url was not called. Check the patch path."
#         assert redis_util.redis == mock_redis_instance, "Redis instance not set correctly."
#         mock_logger.info.assert_called_with('Connected to Redis')


# @pytest.mark.asyncio
# async def test_connect_failure(redis_util):
#     with patch("app.utils.redis_util.aioredis.from_url", side_effect=RedisError("Connection failed")), \
#         patch.object(redis_util, "logger") as mock_logger:

#         with pytest.raises(RedisError, match="Connection failed"):
#             await redis_util.connect()

#         # mock_logger.error.assert_called_with("Error connecting to Redis: Connection failed")
#         assert redis_util.redis is None, "Redis instance should be None on failed connection"


# @pytest.mark.asyncio
# async def test_close_success(redis_util):
#     with patch.object(redis_util, "redis", new=AsyncMock()) as mock_redis_instance, \
#          patch.object(redis_util, "logger") as mock_logger:

#         await redis_util.close()

#         mock_redis_instance.close.assert_called_once()
#         mock_logger.info.assert_called_with('Redis connection closed')


# @pytest.mark.asyncio
# async def test_close_no_connection(redis_util):
#     with patch.object(redis_util, "logger") as mock_logger:
#         redis_util.redis = None

#         await redis_util.close()

#         mock_logger.info.assert_not_called(), "Logger should not log any info if there was no connection to close."


# @pytest.mark.asyncio
# async def test_close_failure(redis_util):
#     with patch.object(redis_util, "redis", new=AsyncMock()) as mock_redis_instance, \
#         patch.object(redis_util, "logger") as mock_logger:

#         mock_redis_instance.close.side_effect = RedisError("Close failed")

#         with pytest.raises(RedisError, match="Close failed"):
#             await redis_util.close()

#         # mock_logger.error.assert_called_with("Error closing Redis connection: Close failed")


# @pytest.mark.asyncio
# async def test_get_existing_key(redis_util):
#     with patch.object(redis_util, "redis", new=AsyncMock()) as mock_redis_instance, \
#          patch.object(redis_util, "logger") as mock_logger:

#         key = "test_key"
#         expected_value = "test_value"
#         mock_redis_instance.get.return_value = expected_value

#         result = await redis_util.get(key)

#         mock_redis_instance.get.assert_called_once_with(key)
#         assert result == expected_value, "Expected value not returned from Redis get."
#         mock_logger.info.assert_called_with(f"Retrieved value for key {key}: {expected_value}")


# @pytest.mark.asyncio
# async def test_get_non_existent_key(redis_util):
#     with patch.object(redis_util, "redis", new=AsyncMock()) as mock_redis_instance, \
#          patch.object(redis_util, "logger") as mock_logger:

#         key = "non_existent_key"
#         mock_redis_instance.get.return_value = None

#         result = await redis_util.get(key)

#         mock_redis_instance.get.assert_called_once_with(key)
#         assert result is None, "Expected None for a non-existent key."
#         mock_logger.info.assert_called_with(f"Retrieved value for key {key}: None")


# @pytest.mark.asyncio
# async def test_get_redis_not_connected(redis_util):
#     with patch.object(redis_util, "logger") as mock_logger:

#         redis_util.redis = None
#         key = "test_key"

#         result = await redis_util.get(key)

#         assert result is None, "Expected None when Redis connection is not established."
#         # mock_logger.error.assert_called_with("Redis connection is not established")


# @pytest.mark.asyncio
# async def test_get_failure(redis_util):
#     with patch.object(redis_util, "redis", new=AsyncMock()) as mock_redis_instance, \
#          patch.object(redis_util, "logger") as mock_logger:

#         key = "test_key"
#         mock_redis_instance.get.side_effect = RedisError("Get failed")

#         with pytest.raises(RedisError, match="Get failed"):
#             await redis_util.get(key)

#         # mock_logger.error.assert_called_with(f"Error getting value for key {key}: Get failed")


# @pytest.mark.asyncio
# async def test_set_success(redis_util):
#     with patch.object(redis_util, "redis", new=AsyncMock()) as mock_redis_instance, \
#          patch.object(redis_util, "logger") as mock_logger:

#         key = "test_key"
#         value = "test_value"

#         await redis_util.set(key, value)

#         mock_redis_instance.set.assert_called_once_with(key, value)
#         mock_logger.info.assert_called_with(f"Set value for key {key}: {value}")


# @pytest.mark.asyncio
# async def test_set_redis_not_connected(redis_util):
#     with patch.object(redis_util, "logger") as mock_logger:

#         redis_util.redis = None
#         key = "test_key"
#         value = "test_value"

#         await redis_util.set(key, value)

#         assert mock_logger.error.call_count == 1

#         mock_logger.error.assert_called_with("Redis connection is not established")

#         # error_call_args = mock_logger.error.call_args[0][0]
#         # assert re.search(r"Redis connection is not established", error_call_args)


# @pytest.mark.asyncio
# async def test_set_failure(redis_util):
#     with patch.object(redis_util, "redis", new=AsyncMock()) as mock_redis_instance, \
#          patch.object(redis_util, "logger") as mock_logger:

#         key = "test_key"
#         value = "test_value"
#         mock_redis_instance.set.side_effect = RedisError("Set failed")

#         with pytest.raises(RedisError, match="Set failed"):
#             await redis_util.set(key, value)

#         mock_logger.error.assert_called_with(f"Error setting value for key {key}: Set failed")


# @pytest.mark.asyncio
# async def test_init_redis_calls_connect(redis_util):
#     with patch.object(redis_util, "connect", new_callable=AsyncMock) as mock_connect:
#         await redis_util.init_redis()
#         mock_connect.assert_called_once()
