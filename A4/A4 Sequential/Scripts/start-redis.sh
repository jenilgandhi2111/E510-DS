echo "Spawning redis server instances"
redis-server ../Config/1/redis.conf &
redis-server ../Config/2/redis.conf &
redis-server ../Config/3/redis.conf &
