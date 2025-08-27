import time
import datetime
import os

def executar_script():
    agora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Executando script às {agora}")

    with open("log_execucao.txt", "a") as log:
        log.write(f"Script executado em: {agora}\n")

    os.system("python main.py")

def verificar_horario():
    while True:
        agora = datetime.datetime.now()
        
        if agora.hour == 6 and agora.minute == 0 and agora.second == 0:
            executar_script()
            time.sleep(1)
        
        time.sleep(0.5)

if __name__ == "__main__":
    print("Monitorando horário... O script rodará às 06:00")
    verificar_horario()
