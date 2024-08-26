const RedisMock = require("ioredis-mock");

const redis = new RedisMock();

const { LuaFactory } = require("wasmoon");

// Initialize a new lua environment factory
// You can pass the wasm location as the first argument, useful if you are using wasmoon on a web environment and want to host the file by yourself
const factory = new LuaFactory();
// Create a standalone lua environment from the factory

async function run() {
  const lua = await factory.createEngine({ injectObjects: true });

  try {
    // Set a JS function to be a global lua function
    lua.global.set("redis", async (cmd, ...args) => {
      const res = await redis[cmd](...args);

      return res;
    });

    // Run a lua string
    const res = await lua.doString(`
    redis = {}

    redis.call = function (...)
      return redisCall(...):await()
    end
    print(redis.call("lpush", "x", "x"))
    print(redis.call("lpush", "x", "y"))
    return redis.call("get", "X")

      
    `);
    console.log(res);
  } finally {
    // Close the lua environment, so it can be freed
    lua.global.close();
  }
}

run();
