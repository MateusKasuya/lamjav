import functions_framework
import sys
import os

# Adicionar diretório atual ao path para Cloud Run
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Agora importar o active_players
from active_players import main


@functions_framework.http
def season_averages(request):
    """NBA Season Averages General Advanced Pipeline"""
    try:
        main()
        return {"status": "success", "message": "Pipeline executed successfully"}
    except Exception as e:
        return {"status": "error", "error": str(e)}, 500
