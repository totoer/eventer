
import asyncio
from eventer.eventer import Eventer


async def main(loop: asyncio.AbstractEventLoop):
    e = Eventer(
        log_filepath=f'emitter.log.pkl', host='localhost', port=9090,
        nodes=[('localhost', 9091,),], loop=loop)
    e.serve()

    await asyncio.sleep(4)

    async def callback(m1: str, m2: str):
        print(f'Emitter: {m1} {m2}')

    e.on('test', callback)

    await e.emit('test', m1='Hello', m2='World')

    await asyncio.sleep(3)

    print('Emmiter master:', e.is_master)

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main(loop))
