import os
import camelot
import matplotlib.pyplot as plt
import pandas as pd

file_name = "injuryreport_dev/etl/pdf_test.pdf"
path = os.path.abspath(file_name)

print("🔍 Testando extração com coordenadas FINAIS...")
print(f"📄 Arquivo: {file_name}")

# Coordenadas FINAIS validadas
page_configs = {
    1: {
        "table_area": "20,493,664,60",  # Página 1
        "columns": ["20,116,198,262,423,584"],
    },
    2: {
        "table_area": "18,521,664,60",  # Página 2 e demais
        "columns": ["18,114,196,260,421,582"],
    },
}

print("📊 Configurações FINAIS:")
print(f"   Página 1 - Table: {page_configs[1]['table_area']}")
print(f"   Página 1 - Columns: {page_configs[1]['columns'][0]}")
print(f"   Página 2+ - Table: {page_configs[2]['table_area']}")
print(f"   Página 2+ - Columns: {page_configs[2]['columns'][0]}")

all_data = []

# Testar extração com configurações finais
for page_num in [1, 2]:
    print(f"\n📄 === TESTANDO PÁGINA {page_num} ===")

    # Escolher configuração baseado na página
    if page_num == 1:
        config = page_configs[1]
    else:
        config = page_configs[2]

    print(f"   Table Area: {config['table_area']}")
    print(f"   Columns: {config['columns'][0]}")

    try:
        tables = camelot.read_pdf(
            path,
            flavor="stream",
            table_areas=[config["table_area"]],
            columns=config["columns"],
            strip_text="\n",
            pages=str(page_num),
        )

        if tables and len(tables) > 0:
            table = tables[0]
            df = table.df

            print(f"   ✅ Extraído: {len(df)} linhas x {len(df.columns)} colunas")
            print(f"   📊 Acurácia: {table.accuracy:.2f}")

            # Adicionar metadados
            df["_page_number"] = page_num
            df["_table_area"] = config["table_area"]
            df["_columns"] = config["columns"][0]

            all_data.append(df)

            # PLOTAR resultado final
            print("   🔍 Plotando resultado FINAL...")
            camelot.plot(table, kind="contour")
            plt.title(f"Página {page_num} - CONFIGURAÇÃO FINAL")
            plt.show()

            print("   📄 Dados extraídos:")
            print(df.head().to_string())

            # Contar células não vazias
            non_empty_cells = 0
            total_cells = len(df) * len(df.columns)
            for col in df.columns:
                if not col.startswith("_"):  # Ignorar metadados
                    non_empty_cells += df[col].astype(str).str.strip().ne("").sum()

            fill_rate = (non_empty_cells / total_cells) * 100 if total_cells > 0 else 0
            print(f"   📈 Taxa de preenchimento: {fill_rate:.1f}%")

        else:
            print(f"   ❌ Nenhuma tabela extraída da página {page_num}")

    except Exception as e:
        print(f"   ❌ Erro na página {page_num}: {e}")

# Combinar todas as páginas
if all_data:
    print("\n📊 === RESULTADO FINAL COMBINADO ===")
    combined_df = pd.concat(all_data, ignore_index=True)

    print(f"✅ Total: {len(combined_df)} linhas x {len(combined_df.columns)} colunas")

    # Mostrar distribuição por página
    if "_page_number" in combined_df.columns:
        page_dist = combined_df["_page_number"].value_counts().sort_index()
        print("📄 Distribuição por página:")
        for page, count in page_dist.items():
            print(f"   Página {page}: {count} linhas")

    print("\n📄 Primeiras 10 linhas combinadas:")
    print(combined_df.head(10).to_string())

    # Salvar resultado final
    combined_df.to_csv("dados_finais_teste.csv", index=False)
    print("\n💾 Dados salvos em: dados_finais_teste.csv")

    print("\n🎉 EXTRAÇÃO FINAL CONCLUÍDA!")
    print("📝 Se os dados estão corretos, o pipeline está pronto para BigQuery!")

else:
    print("\n❌ Nenhum dado foi extraído")

print("\n✅ Teste final concluído!")
