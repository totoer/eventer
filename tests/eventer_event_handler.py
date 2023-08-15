
import asyncio
from eventer.eventer import Eventer


async def main(loop: asyncio.AbstractEventLoop):
    e = Eventer(
        log_filepath=f'handler.log.pkl', host='localhost', port=9091,
        nodes=[('localhost', 9090,),], loop=loop)
    e.serve()

    await asyncio.sleep(3)

    async def callback(m1: str, m2: str):
        print(f'Handler: {m1} {m2}')

    e.on('test', callback)

    await asyncio.sleep(3)

    print('Handler master:', e.is_master)


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main(loop))
