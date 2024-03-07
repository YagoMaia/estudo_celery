from celery import Celery, Task
import redis
import os

r = redis.Redis()

# Setar enviroment
os.environ.setdefault('FORKED_BY_MULTIPROCESSING', '1')

class DebugTask(Task):

    def __call__(self, *args, **kwargs):
        print('TASK STARTING: {0.name}[{0.request.id}]'.format(self))
        return self.run(*args, **kwargs)

#* celery -A tasks worker -l info -P gevent

BROKER_URL = 'redis://localhost:6379/1'
BACKEND_URL = 'redis://localhost:6379/1'
app = Celery('tasks', broker=BROKER_URL, backend=BACKEND_URL, broker_connection_retry_on_startup = True)

@app.task(name = "Soma de dois números", base = DebugTask)
def add(x, y):
    return x + y

@app.task(name = "Hello World", base = DebugTask)
def hello():
    return "Hello World"

@app.task(name = "Inserir Valor redis", base = DebugTask)
def inserir(chave, valor):
    ret = r.set(chave, valor)
    if ret:
        return True
    return False

@app.task(name = "Limpar Cache redis", base = DebugTask)
def flush_redis():
    ret = r.flushdb()
    if ret:
        return "Chaves dos Redis limpas"
    return "Erro ao limpar chaves"