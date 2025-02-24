import os
import pandas as pd
from datetime import datetime, timedelta
from zeep import Client
import re

wsdl = 'https://api.sponteeducacional.net.br/WSAPIEdu.asmx?WSDL'
client = Client(wsdl=wsdl)

hoje = datetime.now()
inicio_semana = hoje - timedelta(days=hoje.weekday())
inicio_ultima_semana = inicio_semana - timedelta(weeks=1)
fim_ultima_semana = inicio_semana - timedelta(days=1)

data_inicio = inicio_ultima_semana.strftime('%d/%m/%Y')
data_fim = fim_ultima_semana.strftime('%d/%m/%Y')

data_inicio_dt = datetime.strptime(data_inicio, '%d/%m/%Y')
data_fim_dt = datetime.strptime(data_fim, '%d/%m/%Y')

def get_turmas_vigentes(data_referencia_dt, dia_semana_referencia_pt, sede, credenciais):
    codigo_cliente = credenciais[sede]['codigo_cliente']
    token = credenciais[sede]['token']

    try:
        response = client.service.GetTurmas(nCodigoCliente=codigo_cliente, sToken=token, sParametrosBusca='Nome= ;')
        turmas_vigentes = []

        for turma in response:
            data_inicio = datetime.strptime(turma.DataInicio, '%d/%m/%Y') if turma.DataInicio else None
            data_termino = datetime.strptime(turma.DataTermino, '%d/%m/%Y') if turma.DataTermino else None
            
            if turma.Situacao == 'Vigente' and turma.Nome != 'Aulas diversas' and turma.Nome != 'Aulas diversas 2' and turma.Nome != 'AULAS DIVERSAS GT':
                if data_inicio and data_termino and data_inicio <= data_referencia_dt <= data_termino:
                    if dia_incluso_em_intervalo(dia_semana_referencia_pt, turma.Horario) or dia_incluso_em_intervalo_caso_de_ruim(dia_semana_referencia_pt, turma.Horario):
                        turmas_vigentes.append(turma)
        return turmas_vigentes

    except Exception as e:
        print(f"Erro ao obter turmas: {e}")
        return []

def gerar_intervalo_dias(inicio, fim):
    dias = []
    while inicio <= fim:
        dias.append(inicio.strftime('%d/%m/%Y'))
        inicio += timedelta(days=1)
    return dias

datas = gerar_intervalo_dias(data_inicio_dt, data_fim_dt)

dias_semana_pt = {
    'Monday': 'Segunda',
    'Tuesday': 'Terça',
    'Wednesday': 'Quarta',
    'Thursday': 'Quinta',
    'Friday': 'Sexta',
    'Saturday': 'Sábado',
    'Sunday': 'Domingo'
}

def parse_intervalo(intervalo):
    dias_semana_pt = {
        'segunda': 'Monday',
        'terça': 'Tuesday',
        'quarta': 'Wednesday',
        'quinta': 'Thursday',
        'sexta': 'Friday',
        'sábado': 'Saturday',
        'domingo': 'Sunday'
    }
    dias_semana_pt_identificados = list(dias_semana_pt.keys())
    
    intervalo = intervalo.lower().strip()
    
    padrao_intervalo = re.compile(
        r'(\bsegunda\b|\bterça\b|\bquarta\b|\bquinta\b|\bsexta\b|\bsábado\b|\bdomingo\b)\s*(?:a|–|-)\s*(\bsegunda\b|\bterça\b|\bquarta\b|\bquinta\b|\bsexta\b|\bsábado\b|\bdomingo\b)', 
        re.IGNORECASE
    )
    intervalos = padrao_intervalo.findall(intervalo)
    
    intervalos_formatados = []
    
    if intervalos:
        for inicio, fim in intervalos:
            if inicio in dias_semana_pt_identificados and fim in dias_semana_pt_identificados:
                indice_inicio = dias_semana_pt_identificados.index(inicio)
                indice_fim = dias_semana_pt_identificados.index(fim)

                dias_intervalo = dias_semana_pt_identificados[indice_inicio:indice_fim + 1]
                intervalos_formatados.extend(dias_intervalo)

    if not intervalos_formatados:
        padrao_dia = re.compile(
            r'\bsegunda\b|\bterça\b|\bquarta\b|\bquinta\b|\bsexta\b|\bsábado\b|\bdomingo\b', 
            re.IGNORECASE
        )
        dias_encontrados = padrao_dia.findall(intervalo)
        dias_encontrados = sorted(set(dias_encontrados), key=lambda x: dias_semana_pt_identificados.index(x))
        intervalos_formatados = dias_encontrados

    return intervalos_formatados

def formatar_intervalo_dias(inicio, fim):
    dias_semana_en = {
        'Segunda': 'Monday',
        'Terça': 'Tuesday',
        'Quarta': 'Wednesday',
        'Quinta': 'Thursday',
        'Sexta': 'Friday',
        'Sábado': 'Saturday',
        'Domingo': 'Sunday'
    }
    dias_semana_pt = {v: k for k, v in dias_semana_en.items()}
    
    dias_semana_pt_identificados = list(dias_semana_pt.keys())
    if inicio in dias_semana_pt_identificados and fim in dias_semana_pt_identificados:
        indice_inicio = dias_semana_pt_identificados.index(inicio)
        indice_fim = dias_semana_pt_identificados.index(fim)
        return dias_semana_pt_identificados[indice_inicio:indice_fim+1]
    return []

def dia_incluso_em_intervalo(dia_semana_pt, intervalo):
    return dia_semana_pt in intervalo

def dia_incluso_em_intervalo_caso_de_ruim(dia_semana_pt, intervalo):
    dias_semana_pt = {
        'Segunda': 'Monday',
        'Terça': 'Tuesday',
        'Quarta': 'Wednesday',
        'Quinta': 'Thursday',
        'Sexta': 'Friday',
        'Sábado': 'Saturday',
        'Domingo': 'Sunday'
    }
    
    dia_en = dias_semana_pt.get(dia_semana_pt, '')
    
    if not dia_en:
        return False

    intervalo = intervalo.lower()
    padrao_dias = re.compile(r'\b(?:segunda|terça|quarta|quinta|sexta|sábado|domingo)\b', re.IGNORECASE)
    dias_encontrados = padrao_dias.findall(intervalo)
    dias_semana_identificados = [dias_semana_pt.get(dia.capitalize(), dia.capitalize()) for dia in dias_encontrados]
    dias_semana_identificados = sorted(set(dias_semana_identificados), key=lambda x: list(dias_semana_pt.values()).index(x))
    
    if not dias_semana_identificados:
        return False

    lista_dias_semana = list(dias_semana_pt.values())
    indice_dia_en = lista_dias_semana.index(dia_en)
    indice_inicio = lista_dias_semana.index(dias_semana_identificados[0])
    indice_fim = lista_dias_semana.index(dias_semana_identificados[-1])

    return indice_inicio <= indice_dia_en <= indice_fim

def formatar_horario(horario):
    horario = horario.lower().strip()
    horario = re.sub(r'\s+', ' ', horario).strip()
    
    intervalo_pattern = re.compile(
        r'(\d{1,2})(?:h|:)?(\d{0,2})?\s*(?:às|a|-\s*das)\s*(\d{1,2})(?:h|:)?(\d{0,2})?', 
        re.IGNORECASE
    )
    intervalos = intervalo_pattern.findall(horario)
    
    horarios_formatados = []
    for inicio_h, inicio_m, fim_h, fim_m in intervalos:
        inicio = f"{inicio_h.zfill(2)}:{inicio_m.zfill(2)}" if inicio_h or inicio_m else ''
        fim = f"{fim_h.zfill(2)}:{fim_m.zfill(2)}" if fim_h or fim_m else ''
        if fim:
            horarios_formatados.append(f"{inicio} às {fim}")
        else:
            horarios_formatados.append(inicio)
    
    horarios_formatados = list(dict.fromkeys(horarios_formatados))
    
    return '; '.join(horarios_formatados)

def formatar_dias_semana(intervalo):
    dias_semana_abreviados = {
        'segunda': 'SEG',
        'terça': 'TER',
        'quarta': 'QUA',
        'quinta': 'QUI',
        'sexta': 'SEX',
        'sábado': 'SAB',
        'domingo': 'DOM'
    }
    
    dias_semana = parse_intervalo(intervalo)
    dias_semana_abrev = [dias_semana_abreviados[dia] for dia in dias_semana]
    return ', '.join(dias_semana_abrev)

def verificar_trancados_turma(turma_id, sede, credenciais):
    codigo_cliente = credenciais[sede]['codigo_cliente']
    token = credenciais[sede]['token']

    try:
        parametros_busca = f"Situacao=5;TurmaID={turma_id};"
        response = client.service.GetMatriculas(nCodigoCliente=codigo_cliente, sToken=token, sParametrosBusca=parametros_busca)
        total_trancados = 0
        
        if response:
            for matricula in response:
                if matricula.Situacao == "Trancado":
                    total_trancados += 1
        
        return total_trancados
    
    except Exception as e:
        print(f"Erro ao buscar matrículas trancadas para a turma {turma_id}: {e}")
        return 0

def get_frequencia_turma(turma_id, parametros_busca, sede, credenciais):
    codigo_cliente = credenciais[sede]['codigo_cliente']
    token = credenciais[sede]['token']

    try:
        response = client.service.GetFrequenciaTurma(
            nCodigoCliente=codigo_cliente,
            sToken=token,
            nTurmaID=turma_id,
            sParametrosBusca=parametros_busca
        )
        frequencias = response if isinstance(response, list) else []
        return frequencias
    
    except Exception as e:
        print(f"Erro ao buscar frequência para a turma {turma_id}: {e}")
        return []

def get_diario_aulas(turma_id, data_referencia_dt, disciplina_id, modulo, sede, credenciais):
    codigo_cliente = credenciais[sede]['codigo_cliente']
    token = credenciais[sede]['token']

    try:
        response = client.service.GetDiarioAulas(
            nCodigoCliente=codigo_cliente,
            sToken=token,
            nTurmaID=turma_id,
            nDisciplinaID=disciplina_id,
            dDataInicio=data_referencia_dt.strftime('%Y-%m-%d'),
            dDataTermino=data_referencia_dt.strftime('%Y-%m-%d'),
            nModulo=modulo
        )

        print(f"Parâmetros de busca para a turma {turma_id}: {disciplina_id}, {modulo}, {data_referencia_dt}, {sede}")
        print(f"Diário de aulas encontrados para a turma {turma_id}: {response}")

        if isinstance(response, list):
            if response:
                for diario in response:
                    retorno = diario['RetornoOperacao']

                    if retorno.startswith("43"):
                        print(f"⚠️ Nenhum registro foi encontrado para a turma {turma_id} ({disciplina_id}, {modulo}, {sede}) na data {data_referencia_dt}.")
                        return None

                    if retorno.startswith("01"):
                        print(f"✅ Operação realizada com sucesso para a turma {turma_id}.")
                        return response

                    if retorno.startswith("02"):
                        print(f"⚠️ Parâmetros inválidos para a turma {turma_id}.")
                        return 'PARÂMETROS_INVÁLIDOS'

            print(f"⚠️ Nenhum diário de aula encontrado para a turma {turma_id}.")
            return []

        print(f"⚠️ A resposta não é uma lista válida. Tipo de resposta: {type(response)}")
        return []

    except Exception as e:
        print(f"Erro ao obter aulas do diário para a turma {turma_id}: {e}")
        return []

def get_quadro_horarios(turma_id, sede, credenciais):
    codigo_cliente = credenciais[sede]['codigo_cliente']
    token = credenciais[sede]['token']
    parametros_busca = f"sTurmaID={turma_id}"

    try:
        response = client.service.GetQuadroHorarios(
            nCodigoCliente=codigo_cliente,
            sToken=token,
            sParametrosBusca=parametros_busca
        )
        return response
    except Exception as e:
        print(f"Erro ao obter quadro: {e}")
        return None

def registrar_problema(nome_turma, data_referencia, motivo):
    arquivo_excel = 'turmas_com_problemas.xlsx'
    novo_registro = pd.DataFrame([[nome_turma, data_referencia, motivo]], columns=['Turma', 'Data Referência', 'Motivo'])

    if os.path.exists(arquivo_excel):
        df_existente = pd.read_excel(arquivo_excel)
        df = pd.concat([df_existente, novo_registro], ignore_index=True)
        df.drop_duplicates(inplace=True)
    else:
        df = novo_registro
    
    df.to_excel(arquivo_excel, index=False)
    print(f"⚠️ Problema registrado para a turma {nome_turma}: {motivo}")