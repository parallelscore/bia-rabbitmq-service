import pytest
from unittest.mock import AsyncMock
from app.utils.redis_util import RedisUtil


class TestRedisUtil:

    @pytest.fixture
    def redis_util(self):
        return RedisUtil()

    @pytest.mark.asyncio
    async def test_redis_util_connect(self, redis_util, caplog):
        await redis_util.connect()
        assert redis_util.redis is not None
        assert 'Connected to Redis' in caplog.text

    @pytest.mark.asyncio
    async def test_redis_util_close(self, redis_util, caplog):
        redis_util.redis = AsyncMock()
        await redis_util.close()
        redis_util.redis.close.assert_awaited_once()
        assert 'Redis connection closed' in caplog.text

    @pytest.mark.asyncio
    async def test_redis_util_get(self, redis_util, caplog):
        redis_util.redis = AsyncMock()
        key = 'test_key'
        value = 'test_value'
        redis_util.redis.get.return_value = value
        result = await redis_util.get(key)
        assert result == value
        redis_util.redis.get.assert_called_once_with(key)
        assert f'Retrieved value for key {key}: {value}' in caplog.text

    @pytest.mark.asyncio
    async def test_redis_util_set(self, redis_util, caplog):
        redis_util.redis = AsyncMock()
        key = 'test_key'
        value = 'test_value'
        await redis_util.set(key, value)
        redis_util.redis.set.assert_called_once_with(key, value)
        assert f'Set value for key {key}: {value}' in caplog.text

    @pytest.mark.asyncio
    async def test_redis_util_init_redis(self, redis_util):
        redis_util.connect = AsyncMock()
        await redis_util.init_redis()
        redis_util.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_redis_util_connect_exception(self, redis_util, caplog):
        redis_util.connect = AsyncMock(side_effect=Exception('Connection failed'))
        with pytest.raises(Exception) as exc_info:
            await redis_util.connect()
        assert 'Connection failed' in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_redis_util_close_exception(self, redis_util):
        redis_util.redis = AsyncMock()
        redis_util.redis.close.side_effect = Exception('Close failed')
        with pytest.raises(Exception) as exc_info:
            await redis_util.close()
        assert 'Close failed' in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_redis_util_get_exception(self, redis_util):
        redis_util.redis = AsyncMock()
        key = 'test_key'
        redis_util.redis.get.side_effect = Exception('Get failed')
        with pytest.raises(Exception) as exc_info:
            await redis_util.get(key)
        assert 'Get failed' in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_redis_util_set_exception(self, redis_util):
        redis_util.redis = AsyncMock()
        key = 'test_key'
        value = 'test_value'
        redis_util.redis.set.side_effect = Exception('Set failed')
        with pytest.raises(Exception) as exc_info:
            await redis_util.set(key, value)
        assert 'Set failed' in str(exc_info.value)

    # Test for init_redis exception handling
    @pytest.mark.asyncio
    async def test_redis_util_init_redis_exception(self, redis_util):
        redis_util.connect = AsyncMock(side_effect=Exception('Init failed'))
        with pytest.raises(Exception) as exc_info:
            await redis_util.init_redis()
        assert 'Init failed' in str(exc_info.value)


if __name__ == '__main__':
    pytest.main()  # pragma: no cover
