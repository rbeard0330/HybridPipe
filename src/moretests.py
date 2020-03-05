import asyncio

def _build_generator(n):
    async def gen():
        my_n = 0
        while my_n < n:
            my_n += 1
            yield my_n
    return gen

async def hello():
    return 4

async def nested_hello():
    return hello()

async def main():
    g = _build_generator(5)()
    print(g)
    async for n in g:
        print(n)
    print(await nested_hello())
    

asyncio.run(main())