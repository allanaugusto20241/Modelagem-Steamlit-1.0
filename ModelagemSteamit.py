# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# FERRAMENTA DE MODELAGEM DE ESTADIAS DE NAVIOS POR POLÍGONOS
# -----------------------------------------------------------------------------
#
# **Objetivo do Script:**
# Este script analisa dados de localização de navios (pontos de GPS) para
# determinar suas estadias em estaleiros. A principal característica é o uso
# de áreas poligonais para definir os limites de cada estaleiro, oferecendo
# alta precisão na detecção.
#
# **Como Funciona:**
# 1.  **Entrada:** Requer um arquivo Excel (.xlsx) com duas abas:
#     - 'Base de dados': Contém os registros de localização dos navios com, no
#       mínimo, nome do navio, data/hora, latitude e longitude.
#     - 'Estaleiros': Define cada estaleiro com um nome e os vértices de seu
#       polígono (ex: lat1, lon1, lat2, lon2, ...).
#
# 2.  **Processamento:**
#     - O script lê os vértices de cada estaleiro e os converte em objetos
#       geométricos do tipo Polígono usando a biblioteca `shapely`.
#     - Para cada registro de localização de navio, ele verifica se o ponto
#       (latitude, longitude) está contido dentro de algum dos polígonos.
#     - Agrupa os registros consecutivos dentro de um mesmo estaleiro para
#       formar "estadias", calculando a data de entrada, saída e duração.
#     - Calcula os períodos "em navegação" entre as estadias.
#
# 3.  **Saída:**
#     - Gera um novo arquivo Excel com um relatório consolidado das estadias
#       e períodos de navegação de cada navio.
#
# **Pré-requisito de Biblioteca:**
# Este script depende da biblioteca 'shapely' para os cálculos geométricos.
# Para instalá-la, execute o comando no seu terminal:
# pip install shapely
#
# -----------------------------------------------------------------------------

# --- Importação de Bibliotecas ---
import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import List, Optional, Dict
import streamlit as st
# A importação mais importante para a lógica de polígonos:
# Point: Representa um único ponto no espaço (coordenada do navio).
# Polygon: Representa uma área bidimensional (a área do estaleiro).
from shapely.geometry import Point, Polygon

# --- Funções Auxiliares ---

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza os nomes das colunas de um DataFrame.

    Converte todos os nomes para letras minúsculas e remove espaços em branco
    no início e no final. Esta é uma prática essencial em Data Science para
    evitar erros de digitação e inconsistências (ex: 'Latitude' vs 'latitude ').

    Args:
        df (pd.DataFrame): O DataFrame a ser normalizado.

    Returns:
        pd.DataFrame: O DataFrame com os nomes das colunas padronizados.
    """
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def _parse_number_maybe(s) -> float:
    """
    Converte uma string ou número em um float de forma robusta, lidando
    tanto com o formato decimal padrão (ex: -43.17) quanto com o formato
    inteiro desformatado (ex: -43172136 -> -43.172136).

    Args:
        s: O valor (string, int, float) a ser convertido.

    Returns:
        float: O valor convertido para float, ou np.nan se a conversão for impossível.
    """
    if pd.isna(s):
        return np.nan

    # ETAPA 1: Tenta converter a entrada para um número float inicial.
    num_val = np.nan
    if isinstance(s, (int, float, np.number)):
        num_val = float(s)
    else:
        raw_str = str(s).strip()
        # Lida com formato brasileiro (ex: '1.234,56' -> '1234.56')
        if ',' in raw_str and re.search(r'\d,\d', raw_str):
            raw_str = raw_str.replace('.', '').replace(',', '.')
        
        raw_str = raw_str.replace(' ', '')
        try:
            num_val = float(raw_str)
        except (ValueError, TypeError):
            # Tenta extrair um número via regex se a conversão direta falhar
            m = re.search(r'-?\d+(?:\.\d+)?', raw_str)
            if m:
                try:
                    num_val = float(m.group(0))
                except (ValueError, TypeError):
                    return np.nan
            else:
                return np.nan

    if pd.isna(num_val):
        return np.nan

    # ETAPA 2: Valida e, se necessário, corrige o número obtido.
    # Uma coordenada válida tem valor absoluto <= 180.
    # Se for maior, verificamos se é um inteiro que precisa de formatação.
    if abs(num_val) > 180:
        # A condição `num_val == int(num_val)` verifica se não há parte fracionária.
        if num_val == int(num_val):
            sign = -1 if num_val < 0 else 1
            s_num = str(abs(int(num_val)))
            
            # Assume o formato DD....... (2 dígitos na parte inteira)
            if len(s_num) > 2:
                integer_part = s_num[:2]
                decimal_part = s_num[2:]
                # Reatribui o valor corrigido a num_val
                num_val = sign * float(f"{integer_part}.{decimal_part}")
            else:
                # É um inteiro grande, mas muito curto para ser uma coordenada.
                return np.nan
        else:
            # É um número grande com casas decimais (ex: 250.5), inválido para coordenadas.
            return np.nan

    return num_val

def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Aplica a conversão numérica a uma lista de colunas de um DataFrame.

    É uma função "helper" que itera sobre uma lista de nomes de colunas e
    aplica a função `_parse_number_maybe` a cada uma delas, garantindo que
    os dados estejam no formato numérico correto para cálculos posteriores.

    Args:
        df (pd.DataFrame): O DataFrame a ser modificado.
        cols (List[str]): Uma lista com os nomes das colunas a serem convertidas.

    Returns:
        pd.DataFrame: O DataFrame com as colunas devidamente convertidas.
    """
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = df[c].map(_parse_number_maybe)
    return df

def _find_sheet_name(xls: pd.ExcelFile, keywords: List[str]) -> Optional[str]:
    """
    Encontra o nome de uma aba (sheet) em um arquivo Excel com base em palavras-chave.

    Torna o script mais robusto a variações no nome do arquivo. Por exemplo,
    tanto 'Base de dados' quanto 'dados da base' seriam encontradas se as
    palavras-chave fossem ['base', 'dados'].

    Args:
        xls (pd.ExcelFile): O objeto do arquivo Excel lido pelo pandas.
        keywords (List[str]): Lista de palavras-chave que devem estar no nome da aba.

    Returns:
        Optional[str]: O nome da aba encontrada ou None.
    """
    # Tenta encontrar uma aba que contenha TODAS as palavras-chave
    def ok(name):
        low = name.strip().lower()
        return all(k in low for k in keywords)
    for s in xls.sheet_names:
        if ok(s):
            return s
    # Se falhar, tenta encontrar uma aba que contenha QUALQUER uma das palavras-chave
    for s in xls.sheet_names:
        low = s.strip().lower()
        if any(k in low for k in keywords):
            return s
    return None

def _find_col(columns: List[str], *alts) -> Optional[str]:
    """
    Encontra o nome de uma coluna com base em uma lista de nomes alternativos possíveis.

    Exemplo: `_find_col(df.columns, 'lon', 'long', 'longitude')` encontrará a coluna
    correta, não importa qual dessas variações foi usada na planilha.

    Args:
        columns (List[str]): A lista de colunas do DataFrame.
        *alts: Nomes alternativos a serem procurados.

    Returns:
        Optional[str]: O nome da coluna encontrada ou None.
    """
    cols = [c.lower().strip() for c in columns]
    for alt in alts:
        for c in cols:
            if alt in c:
                return c
    return None

def _guess_cols_base(base_raw: pd.DataFrame):
    """
    Tenta "adivinhar" os nomes das colunas essenciais na aba da base de dados.

    Automatiza a identificação das colunas de nome do navio, data, latitude e
    longitude, aumentando a flexibilidade do script.
    """
    df = _normalize_cols(base_raw)
    vessel_col = _find_col(df.columns, 'vessel', 'name', 'navio')
    date_col = _find_col(df.columns, 'generated_date', 'data', 'date', 'timestamp', 'hora')
    lat_col = _find_col(df.columns, 'lat')
    lon_col = _find_col(df.columns, 'lon', 'long', 'longitude')
    # Se a busca falhar, usa uma lógica de fallback (ex: primeira ou segunda coluna)
    if vessel_col is None:
        vessel_col = 'vessel_name' if 'vessel_name' in df.columns else df.columns[0]
    if date_col is None:
        candidates = [c for c in df.columns if 'data' in c or 'date' in c or 'time' in c]
        date_col = candidates[0] if candidates else df.columns[1]
    return df, vessel_col, date_col, lat_col, lon_col

def build_stays(df_in: pd.DataFrame, vessel_col: str, date_col: str) -> pd.DataFrame:
    """
    Agrupa registros de presença contíguos em "estadias" consolidadas,
    considerando a sequência completa de localizações do navio.

    Uma "estadia" é um período ininterrupto em um mesmo estaleiro. A função
    usa os registros 'fora do estaleiro' para determinar corretamente o fim
    de uma estadia e o início de um período de navegação.

    Args:
        df_in (pd.DataFrame): O DataFrame COMPLETO com todos os registros,
                              incluindo os 'fora do estaleiro'.
        vessel_col (str): Nome da coluna que identifica o navio.
        date_col (str): Nome da coluna de data/hora.

    Returns:
        pd.DataFrame: DataFrame onde cada linha representa uma estadia completa.
    """
    if df_in.empty:
        return pd.DataFrame(columns=[vessel_col, 'estaleiro', 'data_entrada', 'data_saida', 'tempo_permanencia_dias'])

    # Garante a ordem cronológica, essencial para a lógica de sequência.
    df_sorted = df_in.sort_values([vessel_col, date_col])

    # Cria um ID de bloco/sessão. Um novo ID é gerado toda vez que a localização
    # (seja um estaleiro ou 'fora do estaleiro') muda.
    df_sorted['block_id'] = (df_sorted['estaleiro'].shift() != df_sorted['estaleiro']).cumsum()

    # Agrupa por navio e pelo ID do bloco para consolidar cada período.
    blocks = df_sorted.groupby([vessel_col, 'estaleiro', 'block_id']).agg(
        data_entrada=(date_col, 'min'),
        data_saida=(date_col, 'max')
    ).reset_index()

    # AGORA, removemos os blocos que não são estadias (os de navegação).
    stays = blocks[blocks['estaleiro'] != 'fora do estaleiro'].copy()

    # A coluna 'block_id' foi apenas um auxiliar e pode ser removida.
    stays = stays.drop(columns=['block_id'])
    
    # Se não houver estadias após o filtro, retorna um DataFrame vazio.
    if stays.empty:
        return pd.DataFrame(columns=[vessel_col, 'estaleiro', 'data_entrada', 'data_saida', 'tempo_permanencia_dias'])

    # Adiciona 4 horas à data de saída.
    stays['data_saida'] = stays['data_saida'] + pd.Timedelta(hours=4)

    # Calcula a duração da estadia em dias.
    stays['tempo_permanencia_dias'] = (stays['data_saida'] - stays['data_entrada']).dt.total_seconds() / 86400.0

    return stays

# --- INÍCIO DO SCRIPT PRINCIPAL ---

# ETAPA 1: Leitura e Preparação Inicial dos Dados
# -----------------------------------------------
# Solicita ao usuário que selecione o arquivo Excel.
st.title("Ferramenta de Modelagem de Estadias de Navios")
st.write("Faça o upload do seu arquivo Excel para analisar as estadias.")
in_path = st.file_uploader(
    "Selecione o arquivo Excel (.xlsx)",
    type=['xlsx']
)

if in_path is not None:
    st.info("Arquivo recebido. Processando...")
    xls = pd.ExcelFile(in_path)

    # Tenta encontrar os nomes das abas de forma inteligente.
    base_sheet = _find_sheet_name(xls, ['base', 'dados']) or _find_sheet_name(xls, ['base']) or xls.sheet_names[0]
    estaleiros_sheet = _find_sheet_name(xls, ['estaleiro']) or _find_sheet_name(xls, ['shipyard', 'yard']) or xls.sheet_names[-1]

    # Lê os dados das abas para DataFrames do pandas.
    base_raw = pd.read_excel(xls, sheet_name=base_sheet)
    estaleiros_raw = pd.read_excel(xls, sheet_name=estaleiros_sheet)

    # Limpa e prepara a base de dados dos navios.
    base_df, vessel_col, date_col, base_lat, base_lon = _guess_cols_base(base_raw)
    base_df = _coerce_numeric(base_df, [base_lat, base_lon])
    base_df[date_col] = pd.to_datetime(base_df[date_col], errors='coerce')
    base_df = base_df.dropna(subset=[vessel_col, date_col, base_lat, base_lon]).copy()

    # ETAPA 2: Processamento dos Polígonos dos Estaleiros
    # ----------------------------------------------------
    # Esta é a lógica central da nova abordagem.
    est_df = _normalize_cols(estaleiros_raw)
    yard_name_col = _find_col(est_df.columns, 'estaleiro', 'nome', 'yard')

    # Encontra dinamicamente todas as colunas de vértices (lat1, lon1, lat2, etc.).
    lat_cols = sorted([c for c in est_df.columns if c.startswith('lat')])
    lon_cols = sorted([c for c in est_df.columns if c.startswith('lon')])

    # Garante que todas as coordenadas dos vértices sejam numéricas.
    est_df = _coerce_numeric(est_df, lat_cols + lon_cols)

    # Garante que cada estaleiro na lista tenha um nome definido, removendo linhas
    # em que o nome do estaleiro esteja em branco.
    est_df = est_df.dropna(subset=[yard_name_col]).copy()

    # Cria um dicionário para armazenar os objetos Polígono.
    shipyard_polygons = {}

    # Itera sobre cada linha do DataFrame de estaleiros para construir seu polígono.
    for idx, row in est_df.iterrows():
        vertices = []
        # Usa a função zip para parear as colunas (lat1, lon1), (lat2, lon2), etc.
        # Este loop percorre TODOS os pares de colunas lat/lon.
        for lat_c, lon_c in zip(lat_cols, lon_cols):
            # Apenas adiciona o vértice se AMBOS os valores de latitude e longitude
            # para este par forem válidos (não nulos/vazios).
            # Se um par for inválido (ex: lat3 vazio), ele é ignorado e o loop
            # continua para o próximo par (lat4, lon4), etc.
            if pd.notna(row[lat_c]) and pd.notna(row[lon_c]):
                # O formato exigido por shapely é uma tupla (longitude, latitude).
                vertices.append((row[lon_c], row[lat_c]))

        shipyard_name = row[yard_name_col]
        
        # Um polígono precisa de, no mínimo, 3 vértices.
        if len(vertices) >= 3:
            # Se houver vértices suficientes, o objeto Polígono é criado e armazenado.
            shipyard_polygons[shipyard_name] = Polygon(vertices)
        else:
        # Caso contrário, um aviso é exibido e o estaleiro é ignorado.
            print(f"Aviso: O estaleiro '{shipyard_name}' foi ignorado por ter menos de 3 vértices válidos definidos.")

    # ETAPA 3: Verificação de Presença do Navio nos Polígonos
    # --------------------------------------------------------
    def get_shipyard_location(row: pd.Series, polygons_dict: Dict[str, Polygon], lon_col: str, lat_col: str) -> str:
        """
        Verifica se a coordenada de um navio está dentro de algum polígono de estaleiro.

        Esta função é projetada para ser usada com `df.apply()`.

        Args:
            row: Uma linha do DataFrame `base_df`.
            polygons_dict: O dicionário contendo os objetos Polígono de cada estaleiro.
            lon_col: O nome da coluna de longitude do navio.
            lat_col: O nome da coluna de latitude do navio.

        Returns:
            str: O nome do estaleiro se o navio estiver dentro de um, ou 'fora do estaleiro'.
        """
        # Cria um objeto Point para a localização atual do navio.
        point = Point(row[lon_col], row[lat_col])
        # Itera sobre cada polígono de estaleiro.
        for name, polygon in polygons_dict.items():
            # A função .contains() é o núcleo da verificação geométrica.
            # Ela retorna True se o ponto estiver dentro ou na fronteira do polígono.
            if polygon.contains(point):
                return name  # Retorna o nome do estaleiro e para a verificação.
        return 'fora do estaleiro'

    # Aplica a função de verificação a cada linha do DataFrame de navios.
    # O resultado é uma nova coluna 'estaleiro' que armazena a localização de cada registro.
    # `axis=1` garante que a função receba cada linha individualmente.
    base_df['estaleiro'] = base_df.apply(
        get_shipyard_location,
        args=(shipyard_polygons, base_lon, base_lat), # Argumentos extras para a função
        axis=1
    )

    # Cria o DataFrame `presence_df` contendo apenas os registros onde o navio
    # foi detectado dentro de um estaleiro.
    presence_df = base_df[base_df['estaleiro'] != 'fora do estaleiro'].copy()

    # ETAPA 4: Construção das Estadias Consolidadas
    # ----------------------------------------------
    # A função `build_stays` agora recebe o DataFrame completo (`base_df`)
    # para analisar a sequência cronológica real, incluindo os períodos de navegação.
    stays_df = build_stays(base_df, vessel_col, date_col)

    # ETAPA 5: Cálculo dos Períodos de Navegação
    # -------------------------------------------
    # Esta etapa analisa as lacunas de tempo ENTRE as estadias para identificar
    # quando os navios estavam se movendo de um local para outro.
    navigation_records = []
    if not stays_df.empty:
        stays_df_sorted = stays_df.sort_values([vessel_col, 'data_entrada']).reset_index(drop=True)
        # Agrupa por navio para analisar a sequência de estadias de cada um.
        for vessel_name, group in stays_df_sorted.groupby(vessel_col):
            # A função .shift(1) "puxa" o valor da linha anterior para a linha atual.
            # Isso nos permite comparar a estadia atual com a anterior do mesmo navio.
            previous_exit_time = group['data_saida'].shift(1)
            for idx, row in group.iterrows():
                if pd.notna(previous_exit_time.loc[idx]):
                    current_entry_time = row['data_entrada']
                    prev_exit = previous_exit_time.loc[idx]
                    
                    # Se a entrada na estadia atual é posterior à saída da anterior,
                    # o tempo entre elas foi um período de navegação.
                    if current_entry_time > prev_exit:
                        duration_d = (current_entry_time - prev_exit).total_seconds() / 86400.0
                        navigation_records.append({
                            vessel_col: vessel_name,
                            'estaleiro': 'em navegação',
                            'data_entrada': prev_exit,
                            'data_saida': current_entry_time,
                            'tempo_permanencia_dias': duration_d
                        })

    # Junta os dados de estadias com os de navegação em um único DataFrame.
    if navigation_records:
        navigation_df = pd.DataFrame(navigation_records)
        combined_df = pd.concat([stays_df, navigation_df], ignore_index=True)
    else:
        combined_df = stays_df

    # ETAPA 6: Formatação Final e Exportação do Relatório
    # ---------------------------------------------------
    if not combined_df.empty:
        final_df = combined_df.sort_values(['estaleiro', vessel_col, 'data_entrada']).reset_index(drop=True)
        
        # Renomeia as colunas para um formato mais claro e profissional no relatório final.
        col_map = {
            vessel_col: 'Nome do navio',
            'estaleiro': 'Nome do estaleiro',
            'data_entrada': 'Data de entrada',
            'data_saida': 'Data de saída',
            'tempo_permanencia_dias': 'Tempo de permanência (d)'
        }
        final_df = final_df.rename(columns=col_map)
        
        # Salva o resultado em um novo arquivo Excel.
        if not final_df.empty:
            st.success("Processamento concluído com sucesso!")
            st.dataframe(final_df) # Mostra a tabela de resultados na tela

        # Converte o DataFrame para Excel em memória
            @st.cache_data
            def convert_df_to_excel(df):
                from io import BytesIO
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df.to_excel(writer, index=False, sheet_name='Estadias')
                return output.getvalue()
    
            excel_data = convert_df_to_excel(final_df)
    
            st.download_button(
                label="📥 Baixar Relatório em Excel",
                data=excel_data,
                file_name=f'modelagem_estadias_{in_path.name}',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
    else:
        st.warning("Nenhuma estadia foi detectada.")