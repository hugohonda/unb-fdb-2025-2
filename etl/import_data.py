#!/usr/bin/env python3
"""
Script ETL para importação dos dados de preços de medicamentos
do arquivo CSV para o banco de dados relacional PostgreSQL
"""

import csv
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import sys
from decimal import Decimal, InvalidOperation


class MedicamentosETL:
    """Classe para realizar o processo ETL dos dados de medicamentos"""
    
    def __init__(self, host, database, user, password, csv_file):
        """
        Inicializa conexão com banco de dados PostgreSQL e arquivo CSV
        
        Args:
            host: Host do banco de dados
            database: Nome do banco de dados
            user: Usuário do banco
            password: Senha do banco
            csv_file: Caminho para arquivo CSV
        """
        self.csv_file = csv_file
        self.connection = None
        self.cursor = None
        
        try:
            self.connection = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            print(f"✓ Conectado ao banco de dados {database}")
        except Exception as e:
            print(f"✗ Erro ao conectar ao banco: {e}")
            sys.exit(1)
    
    def limpar_valor_numerico(self, valor):
        """Converte string numérica para Decimal, tratando vírgulas e valores vazios"""
        if not valor or valor.strip() == '' or valor.strip() == '-' or valor.strip() == '    -     ':
            return None
        
        # Remove espaços e substitui vírgula por ponto
        valor_limpo = valor.strip().replace(',', '.')
        
        try:
            return Decimal(valor_limpo)
        except (InvalidOperation, ValueError):
            return None
    
    def obter_ou_criar_id(self, tabela, campo, valor, campos_extra=None):
        """
        Obtém ID de um registro ou cria se não existir
        
        Args:
            tabela: Nome da tabela
            campo: Nome do campo para buscar
            valor: Valor a buscar
            campos_extra: Dict com campos adicionais para inserção
        """
        if not valor or valor.strip() == '':
            return None
        
        # Extrai nome do ID da tabela
        nome_id = f"id_{tabela.rstrip('s')}"
        
        # Busca registro existente
        query = f'SELECT {nome_id} as id FROM {tabela} WHERE {campo} = %s'
        self.cursor.execute(query, (valor,))
        resultado = self.cursor.fetchone()
        
        if resultado:
            return resultado['id']
        
        # Cria novo registro
        campos = [campo]
        valores = [valor]
        
        if campos_extra:
            campos.extend(campos_extra.keys())
            valores.extend(campos_extra.values())
        
        placeholders = ', '.join(['%s'] * len(campos))
        campos_str = ', '.join(campos)
        
        insert_query = f'INSERT INTO {tabela} ({campos_str}) VALUES ({placeholders}) RETURNING {nome_id}'
        self.cursor.execute(insert_query, valores)
        return self.cursor.fetchone()['id']
    
    def processar_aliquotas_icms(self):
        """Processa e insere todas as alíquotas de ICMS possíveis"""
        aliquotas = [
            (0, 'Isento'),
            (12, '12%'),
            (17, '17%'),
            (17.5, '17,5%'),
            (18, '18%'),
            (19, '19%'),
            (19.5, '19,5%'),
            (20, '20%'),
            (20.5, '20,5%'),
            (21, '21%'),
            (22, '22%'),
            (22.5, '22,5%'),
            (23, '23%'),
        ]
        
        for aliquota, descricao in aliquotas:
            self.obter_ou_criar_id('aliquotas_icms', 'aliquota', Decimal(str(aliquota)), 
                                   {'descricao': descricao})
        
        print("✓ Alíquotas de ICMS processadas")
    
    def processar_linha_csv(self, linha, linha_num):
        """Processa uma linha do CSV e insere no banco de dados"""
        try:
            # Campos principais
            substancia = linha[0].strip() if len(linha) > 0 else ''
            cnpj = linha[1].strip() if len(linha) > 1 else ''
            laboratorio = linha[2].strip() if len(linha) > 2 else ''
            codigo_ggrem = linha[3].strip() if len(linha) > 3 else ''
            registro = linha[4].strip() if len(linha) > 4 else ''
            ean1 = linha[5].strip() if len(linha) > 5 else ''
            ean2 = linha[6].strip() if len(linha) > 6 else ''
            ean3 = linha[7].strip() if len(linha) > 7 else ''
            produto = linha[8].strip() if len(linha) > 8 else ''
            apresentacao = linha[9].strip() if len(linha) > 9 else ''
            classe_terapeutica = linha[10].strip() if len(linha) > 10 else ''
            tipo_produto = linha[11].strip() if len(linha) > 11 else ''
            regime_preco = linha[12].strip() if len(linha) > 12 else ''
            
            # Validações básicas
            if not substancia or not codigo_ggrem or not produto:
                return False
            
            # Processa entidades relacionais
            id_substancia = self.obter_ou_criar_id('substancias', 'nome_substancia', substancia)
            id_laboratorio = self.obter_ou_criar_id('laboratorios', 'cnpj', cnpj, 
                                                   {'nome_laboratorio': laboratorio})
            
            # Classe terapêutica pode ter código e descrição separados
            partes_classe = classe_terapeutica.split(' - ', 1)
            codigo_classe = partes_classe[0] if partes_classe else classe_terapeutica
            descricao_classe = partes_classe[1] if len(partes_classe) > 1 else classe_terapeutica
            
            id_classe = self.obter_ou_criar_id('classes_terapeuticas', 'codigo_classe', codigo_classe,
                                              {'descricao_classe': descricao_classe})
            
            id_tipo = self.obter_ou_criar_id('tipos_produto', 'tipo_produto', tipo_produto)
            id_regime = self.obter_ou_criar_id('regimes_preco', 'regime_preco', regime_preco)
            
            # Campos adicionais do produto
            campos_adicionais_idx = 65
            restricao_hospitalar = linha[campos_adicionais_idx].strip() if len(linha) > campos_adicionais_idx else 'Não'
            cap = linha[campos_adicionais_idx + 1].strip() if len(linha) > campos_adicionais_idx + 1 else 'Não'
            confaz87 = linha[campos_adicionais_idx + 2].strip() if len(linha) > campos_adicionais_idx + 2 else 'Não'
            icms_zero = linha[campos_adicionais_idx + 3].strip() if len(linha) > campos_adicionais_idx + 3 else 'Não'
            analise_recursal = linha[campos_adicionais_idx + 4].strip() if len(linha) > campos_adicionais_idx + 4 else ''
            lista_credito = linha[campos_adicionais_idx + 5].strip() if len(linha) > campos_adicionais_idx + 5 else ''
            comercializacao = linha[campos_adicionais_idx + 6].strip() if len(linha) > campos_adicionais_idx + 6 else 'Não'
            tarja = linha[campos_adicionais_idx + 7].strip() if len(linha) > campos_adicionais_idx + 7 else ''
            
            # Normaliza valores enum
            restricao_hospitalar = 'Sim' if restricao_hospitalar.upper() == 'SIM' else 'Não especificado'
            if restricao_hospitalar not in ['Sim', 'Não', 'Não especificado']:
                restricao_hospitalar = 'Não especificado'
            cap = 'Sim' if cap.upper() == 'SIM' else 'Não'
            confaz87 = 'Sim' if confaz87.upper() == 'SIM' else 'Sim' if 'CONFAZ' in str(confaz87).upper() else 'Não'
            icms_zero = 'Sim' if icms_zero.upper() == 'SIM' else 'Não'
            comercializacao = 'Sim' if comercializacao.upper() == 'SIM' else 'Não'
            
            # Verifica se produto já existe
            self.cursor.execute("SELECT id_produto FROM produtos WHERE codigo_ggrem = %s", (codigo_ggrem,))
            produto_existente = self.cursor.fetchone()
            id_produto_existente = produto_existente['id_produto'] if produto_existente else None
            
            if id_produto_existente:
                # Atualiza produto existente
                query_produto = """
                    UPDATE produtos SET
                        registro = %s, ean_1 = %s, ean_2 = %s, ean_3 = %s, nome_produto = %s, apresentacao = %s,
                        id_substancia = %s, id_laboratorio = %s, id_classe = %s, id_tipo = %s, id_regime = %s,
                        restricao_hospitalar = %s::tipo_restricao, cap = %s::tipo_sim_nao, confaz_87 = %s::tipo_sim_nao, 
                        icms_zero = %s::tipo_sim_nao, analise_recursal = %s,
                        lista_concessao_credito = %s, comercializacao_2024 = %s::tipo_sim_nao, tarja = %s
                    WHERE codigo_ggrem = %s
                """
                valores_produto = (
                    registro or None, ean1 or None, ean2 or None, ean3 or None,
                    produto, apresentacao, id_substancia, id_laboratorio, id_classe,
                    id_tipo, id_regime, restricao_hospitalar, cap, confaz87, icms_zero,
                    analise_recursal or None, lista_credito or None, comercializacao, tarja or None,
                    codigo_ggrem
                )
                self.cursor.execute(query_produto, valores_produto)
                id_produto = id_produto_existente
            else:
                # Insere novo produto
                query_produto = """
                    INSERT INTO produtos (
                        codigo_ggrem, registro, ean_1, ean_2, ean_3, nome_produto, apresentacao,
                        id_substancia, id_laboratorio, id_classe, id_tipo, id_regime,
                        restricao_hospitalar, cap, confaz_87, icms_zero, analise_recursal,
                        lista_concessao_credito, comercializacao_2024, tarja
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::tipo_restricao, %s::tipo_sim_nao, %s::tipo_sim_nao, %s::tipo_sim_nao, %s, %s, %s::tipo_sim_nao, %s)
                    RETURNING id_produto
                """
                valores_produto = (
                    codigo_ggrem, registro or None, ean1 or None, ean2 or None, ean3 or None,
                    produto, apresentacao, id_substancia, id_laboratorio, id_classe,
                    id_tipo, id_regime, restricao_hospitalar, cap, confaz87, icms_zero,
                    analise_recursal or None, lista_credito or None, comercializacao, tarja or None
                )
                self.cursor.execute(query_produto, valores_produto)
                id_produto = self.cursor.fetchone()['id_produto']
            
            if not id_produto:
                return False
            
            # Processa preços PF
            aliquotas_pf = [
                (None, 13, 'PF Sem Impostos'),
                (0, 14, 'PF 0%'),
                (12, 15, 'PF 12%'),
                (12, 16, 'PF 12% ALC'),
                (17, 17, 'PF 17%'),
                (17, 18, 'PF 17% ALC'),
                (17.5, 19, 'PF 17.5%'),
                (17.5, 20, 'PF 17.5% ALC'),
                (18, 21, 'PF 18%'),
                (18, 22, 'PF 18% ALC'),
                (19, 23, 'PF 19%'),
                (19, 24, 'PF 19% ALC'),
                (19.5, 25, 'PF 19.5%'),
                (19.5, 26, 'PF 19.5% ALC'),
                (20, 27, 'PF 20%'),
                (20, 28, 'PF 20% ALC'),
                (20.5, 29, 'PF 20.5%'),
                (20.5, 30, 'PF 20.5% ALC'),
                (21, 31, 'PF 21%'),
                (21, 32, 'PF 21% ALC'),
                (22, 33, 'PF 22%'),
                (22, 34, 'PF 22% ALC'),
                (22.5, 35, 'PF 22.5%'),
                (22.5, 36, 'PF 22.5% ALC'),
                (23, 37, 'PF 23%'),
                (23, 38, 'PF 23% ALC'),
            ]
            
            for aliquota_val, idx, descricao in aliquotas_pf:
                if len(linha) > idx:
                    pf_sem_impostos = self.limpar_valor_numerico(linha[idx]) if idx == 13 else None
                    pf_com_impostos = self.limpar_valor_numerico(linha[idx]) if idx != 13 else None
                    
                    if pf_com_impostos or pf_sem_impostos:
                        id_aliquota = None
                        if aliquota_val is not None:
                            self.cursor.execute("SELECT id_aliquota FROM aliquotas_icms WHERE aliquota = %s", 
                                              (Decimal(str(aliquota_val)),))
                            result = self.cursor.fetchone()
                            id_aliquota = result['id_aliquota'] if result else None
                        
                        if id_aliquota is not None or aliquota_val is None:
                            query_preco = """
                                INSERT INTO precos_fabrica 
                                    (id_produto, id_aliquota, pf_sem_impostos, pf_com_impostos, data_vigencia)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (id_produto, id_aliquota, data_vigencia)
                                DO UPDATE SET
                                    pf_sem_impostos = EXCLUDED.pf_sem_impostos,
                                    pf_com_impostos = EXCLUDED.pf_com_impostos
                            """
                            self.cursor.execute(query_preco, (
                                id_produto, id_aliquota, 
                                float(pf_sem_impostos) if pf_sem_impostos else None,
                                float(pf_com_impostos) if pf_com_impostos else None,
                                datetime.now().date()
                            ))
            
            # Processa preços PMVG
            aliquotas_pmvg = [
                (None, 39, 'PMVG Sem Impostos'),
                (0, 40, 'PMVG 0%'),
                (12, 41, 'PMVG 12%'),
                (12, 42, 'PMVG 12% ALC'),
                (17, 43, 'PMVG 17%'),
                (17, 44, 'PMVG 17% ALC'),
                (17.5, 45, 'PMVG 17.5%'),
                (17.5, 46, 'PMVG 17.5% ALC'),
                (18, 47, 'PMVG 18%'),
                (18, 48, 'PMVG 18% ALC'),
                (19, 49, 'PMVG 19%'),
                (19, 50, 'PMVG 19% ALC'),
                (19.5, 51, 'PMVG 19.5%'),
                (19.5, 52, 'PMVG 19.5% ALC'),
                (20, 53, 'PMVG 20%'),
                (20, 54, 'PMVG 20% ALC'),
                (20.5, 55, 'PMVG 20.5%'),
                (20.5, 56, 'PMVG 20.5% ALC'),
                (21, 57, 'PMVG 21%'),
                (21, 58, 'PMVG 21% ALC'),
                (22, 59, 'PMVG 22%'),
                (22, 60, 'PMVG 22% ALC'),
                (22.5, 61, 'PMVG 22.5%'),
                (22.5, 62, 'PMVG 22.5% ALC'),
                (23, 63, 'PMVG 23%'),
                (23, 64, 'PMVG 23% ALC'),
            ]
            
            for aliquota_val, idx, descricao in aliquotas_pmvg:
                if len(linha) > idx:
                    pmvg_sem_impostos = self.limpar_valor_numerico(linha[idx]) if idx == 39 else None
                    pmvg_com_impostos = self.limpar_valor_numerico(linha[idx]) if idx != 39 else None
                    
                    if pmvg_com_impostos or pmvg_sem_impostos:
                        id_aliquota = None
                        if aliquota_val is not None:
                            self.cursor.execute("SELECT id_aliquota FROM aliquotas_icms WHERE aliquota = %s", 
                                              (Decimal(str(aliquota_val)),))
                            result = self.cursor.fetchone()
                            id_aliquota = result['id_aliquota'] if result else None
                        
                        if id_aliquota is not None or aliquota_val is None:
                            query_preco = """
                                INSERT INTO precos_pmvg 
                                    (id_produto, id_aliquota, pmvg_sem_impostos, pmvg_com_impostos, data_vigencia)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (id_produto, id_aliquota, data_vigencia)
                                DO UPDATE SET
                                    pmvg_sem_impostos = EXCLUDED.pmvg_sem_impostos,
                                    pmvg_com_impostos = EXCLUDED.pmvg_com_impostos
                            """
                            self.cursor.execute(query_preco, (
                                id_produto, id_aliquota,
                                float(pmvg_sem_impostos) if pmvg_sem_impostos else None,
                                float(pmvg_com_impostos) if pmvg_com_impostos else None,
                                datetime.now().date()
                            ))
            
            return True
            
        except Exception as e:
            print(f"✗ Erro ao processar linha {linha_num}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def executar_etl(self, pular_linhas=72):
        """Executa o processo completo de ETL"""
        print(f"\nIniciando processo ETL do arquivo: {self.csv_file}")
        print(f"Pulando {pular_linhas} linhas de cabeçalho...\n")
        
        # Processa alíquotas primeiro
        self.processar_aliquotas_icms()
        self.connection.commit()
        
        linhas_processadas = 0
        linhas_sucesso = 0
        linhas_erro = 0
        
        try:
            with open(self.csv_file, 'r', encoding='utf-8', errors='ignore') as arquivo:
                leitor = csv.reader(arquivo, delimiter=';')
                
                # Pula linhas de cabeçalho
                for _ in range(pular_linhas):
                    next(leitor, None)
                
                for linha_num, linha in enumerate(leitor, start=pular_linhas + 1):
                    if len(linha) < 10:
                        continue
                    
                    linhas_processadas += 1
                    
                    if self.processar_linha_csv(linha, linha_num):
                        linhas_sucesso += 1
                    else:
                        linhas_erro += 1
                    
                    # Commit a cada 100 linhas
                    if linhas_processadas % 100 == 0:
                        self.connection.commit()
                        print(f"Processadas {linhas_processadas} linhas... (Sucesso: {linhas_sucesso}, Erro: {linhas_erro})")
                
                # Commit final
                self.connection.commit()
                
        except Exception as e:
            print(f"\n✗ Erro durante processamento: {e}")
            import traceback
            traceback.print_exc()
            self.connection.rollback()
            raise
        
        print(f"\n✓ Processo ETL concluído!")
        print(f"  Total de linhas processadas: {linhas_processadas}")
        print(f"  Linhas com sucesso: {linhas_sucesso}")
        print(f"  Linhas com erro: {linhas_erro}")
    
    def fechar(self):
        """Fecha conexão com banco de dados"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("✓ Conexão fechada")


def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL para importação de dados de medicamentos (PostgreSQL)')
    parser.add_argument('--host', default='localhost', help='Host do banco de dados')
    parser.add_argument('--database', default='medicamentos_gov', help='Nome do banco de dados')
    parser.add_argument('--user', required=True, help='Usuário do banco de dados')
    parser.add_argument('--password', required=True, help='Senha do banco de dados')
    parser.add_argument('--csv', default='TA_PRECO_MEDICAMENTO_GOV.csv', help='Arquivo CSV para importar')
    parser.add_argument('--skip', type=int, default=72, help='Número de linhas a pular (cabeçalho)')
    
    args = parser.parse_args()
    
    etl = MedicamentosETL(args.host, args.database, args.user, args.password, args.csv)
    
    try:
        etl.executar_etl(pular_linhas=args.skip)
    finally:
        etl.fechar()


if __name__ == '__main__':
    main()
