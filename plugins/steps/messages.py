from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(token='7661951958:AAFpEruQ4Zd6-Z0SPAU6bzF62G8We1Q2Vmc', chat_id='-4758804067')
    dag = context['dag'].dag_id
    run_id = context['run_id']
            
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '-4758804067',
        'text': message
    })     

def send_telegram_failure_message(context):
    hook = TelegramHook(token='7661951958:AAFpEruQ4Zd6-Z0SPAU6bzF62G8We1Q2Vmc', chat_id='-4758804067')
    dag = context['dag'].dag_id
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
            
    message = f'Исполнение DAG {dag} с id {run_id} провалилось {task_instance_key_str}'
    hook.send_message({
        'chat_id': '-4758804067',
        'text': message
    })  