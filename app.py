from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client, Client
from dotenv import load_dotenv
import datetime
import os
import json
import pandas as pd

# ------------------------------------------------------------
# 🔧 CONFIGURACIÓN INICIAL
# ------------------------------------------------------------
load_dotenv()

app = Flask(__name__)
CORS(app)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Inicializar cliente de Supabase
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Cliente Supabase inicializado correctamente.")
except Exception as e:
    print(f"❌ Error al inicializar Supabase: {e}")

# ------------------------------------------------------------
# 🚀 1. DATASETS
# ------------------------------------------------------------
@app.route('/api/datasets', methods=['GET'])
def get_datasets():
    response = supabase.table("datasets").select("*").order("fecha_carga", desc=True).execute()
    return jsonify(response.data)


@app.route('/api/datasets', methods=['POST'])
def create_dataset():
    """📦 Registra un nuevo dataset (CSV o JSON) en Supabase."""
    try:
        if 'file' in request.files:
            file = request.files['file']
            filename = file.filename

            if not filename:
                return jsonify({"message": "El archivo no tiene nombre válido."}), 400

            os.makedirs("temp", exist_ok=True)
            temp_path = os.path.join("temp", filename)
            file.save(temp_path)

            # Leer archivo
            try:
                if filename.endswith('.csv'):
                    try:
                        df = pd.read_csv(temp_path, encoding='utf-8', on_bad_lines='skip')
                    except UnicodeDecodeError:
                        df = pd.read_csv(temp_path, encoding='latin-1', on_bad_lines='skip')
                elif filename.endswith('.json'):
                    df = pd.read_json(temp_path)
                else:
                    return jsonify({"message": "Formato no soportado. Solo CSV o JSON."}), 400
            except Exception as e:
                return jsonify({"message": f"Error al procesar archivo: {str(e)}"}), 400

            num_filas, num_columnas = df.shape
            storage_path = f"datasets/{filename}"

            # Subir al bucket datasets
            try:
                with open(temp_path, "rb") as f:
                    supabase.storage.from_("datasets").upload(
                        storage_path,
                        f,
                        file_options={"content-type": "text/csv"}
                    )
            except Exception as e:
                return jsonify({"message": f"Error al subir archivo a Storage: {str(e)}"}), 500

            # Calcular estadísticas simples
            nulls = int(df.isnull().sum().sum())
            duplicates = int(df.duplicated().sum())
            outliers = 0  # (simple placeholder)

            # Registrar en la tabla datasets
            response = supabase.table("datasets").insert({
                "nombre": filename,
                "ruta_almacenamiento": storage_path,
                "num_filas": num_filas,
                "num_columnas": num_columnas,
                "metadata_json": {},
                "fecha_carga": datetime.datetime.now().isoformat()
            }).execute()

            os.remove(temp_path)

            return jsonify({
                "id": response.data[0]["id"],
                "nombre": filename,
                "total_filas": num_filas,
                "total_columnas": num_columnas,
                "nulls": nulls,
                "duplicados": duplicates,
                "outliers": outliers,
                "preview": df.head(20).to_dict(orient="records")
            }), 201

        elif request.is_json:
            data = request.get_json()
            if not data or 'nombre' not in data:
                return jsonify({"message": "Faltan datos requeridos (nombre)."}), 400

            response = supabase.table("datasets").insert({
                "nombre": data['nombre'],
                "ruta_almacenamiento": data.get('ruta_almacenamiento'),
                "num_filas": data.get('num_filas'),
                "num_columnas": data.get('num_columnas'),
                "metadata_json": data.get('metadata_json', {}),
                "fecha_carga": datetime.datetime.now().isoformat()
            }).execute()

            return jsonify({
                "id": response.data[0]['id'],
                "nombre": response.data[0]['nombre']
            }), 201

        else:
            return jsonify({"message": "Debe enviar un archivo o un JSON válido."}), 400

    except Exception as e:
        return jsonify({"message": f"Error general al crear dataset: {str(e)}"}), 500


# ------------------------------------------------------------
# 🚀 2. LIMPIEZAS DE DATOS
# ------------------------------------------------------------
@app.route('/api/limpiezas', methods=['GET'])
def get_limpiezas():
    response = supabase.table("limpiezas_datos").select("*").order("fecha_limpieza", desc=True).execute()
    return jsonify(response.data)


@app.route('/api/limpiezas', methods=['POST'])
def limpiar_dataset_multiple():
    """
    Recibe:
    {
        "dataset_id": 1,
        "tipos_limpieza": [
            {"tipo": "nulos"},
            {"tipo": "duplicados"},
            {"tipo": "outliers"},
            {"tipo": "normalizacion"}
        ]
    }
    """
    try:
        data = request.get_json(force=True)
        dataset_id = data.get("dataset_id")
        tipos_limpieza = data.get("tipos_limpieza", [])

        if not dataset_id or not tipos_limpieza:
            return jsonify({"error": "Debe incluir dataset_id y una lista de tipos_limpieza."}), 400

        # 🔹 Buscar dataset en Supabase
        dataset_res = supabase.table("datasets").select("*").eq("id", dataset_id).single().execute()
        if not dataset_res.data:
            return jsonify({"error": "Dataset no encontrado."}), 404

        dataset = dataset_res.data
        ruta_dataset = dataset["ruta_almacenamiento"]

        # 🔹 Descargar CSV original
        response = supabase.storage.from_("datasets").download(ruta_dataset)
        temp_path = f"temp/{os.path.basename(ruta_dataset)}"
        os.makedirs("temp", exist_ok=True)
        with open(temp_path, "wb") as f:
            f.write(response)

        try:
            df = pd.read_csv(temp_path, on_bad_lines='skip', encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(temp_path, on_bad_lines='skip', encoding='latin-1')

        # Guardar copia original
        original_len = len(df)
        original_cols = len(df.columns)

        operaciones_realizadas = []
        total_afectados = 0

        # 🔧 Aplicar cada tipo de limpieza
        for limpieza in tipos_limpieza:
            if isinstance(limpieza, str):
                tipo = limpieza
                parametros = {}
            elif isinstance(limpieza, dict):
                tipo = limpieza.get("tipo")
                parametros = limpieza.get("parametros", {})
            else:
                continue

            afectados = 0

            if tipo == "duplicados":
                antes = len(df)
                df = df.drop_duplicates()
                afectados = antes - len(df)

            elif tipo == "nulos":
                metodo = parametros.get("metodo", "drop")
                antes = len(df)
                if metodo == "drop":
                    df = df.dropna()
                    afectados = antes - len(df)
                elif metodo in ["ffill", "bfill"]:
                    df = df.fillna(method=metodo)
                elif metodo == "mean":
                    df = df.fillna(df.mean(numeric_only=True))

            elif tipo == "outliers":
                columnas = parametros.get("columnas", list(df.select_dtypes(include=['float64', 'int64']).columns))
                umbral = parametros.get("umbral", 1.5)
                antes = len(df)
                for col in columnas:
                    if df[col].dtype in ['float64', 'int64']:
                        Q1 = df[col].quantile(0.25)
                        Q3 = df[col].quantile(0.75)
                        IQR = Q3 - Q1
                        lower, upper = Q1 - umbral * IQR, Q3 + umbral * IQR
                        df = df[(df[col] >= lower) & (df[col] <= upper)]
                afectados = antes - len(df)

            elif tipo == "normalizacion":
                columnas = parametros.get("columnas", list(df.select_dtypes(include=['float64', 'int64']).columns))
                for col in columnas:
                    if df[col].dtype in ['float64', 'int64']:
                        min_val, max_val = df[col].min(), df[col].max()
                        if max_val != min_val:
                            df[col] = (df[col] - min_val) / (max_val - min_val)

            operaciones_realizadas.append({
                "tipo": tipo,
                "parametros": parametros,
                "afectados": int(afectados)
            })
            total_afectados += afectados

        # 🔹 Guardar dataset limpio
        clean_filename = f"clean_multi_{os.path.basename(ruta_dataset)}"
        clean_path = f"temp/{clean_filename}"
        df.to_csv(clean_path, index=False)
        ruta_storage_clean = f"clean/{clean_filename}"

        # Subir el archivo limpio
        with open(clean_path, "rb") as f:
            try:
                supabase.storage.from_("datasets").remove([ruta_storage_clean])
            except Exception:
                pass
            supabase.storage.from_("datasets").upload(
                ruta_storage_clean,
                f,
                file_options={"content-type": "text/csv"}
            )

        # Registrar limpieza
        limpieza_insert = supabase.table("limpiezas_datos").insert({
            "dataset_id": dataset_id,
            "tipo_limpieza": "multiple",
            "parametros_usados": operaciones_realizadas,
            "num_registros_afectados": int(total_afectados),
            "ruta_dataset_limpio": ruta_storage_clean,
            "estado": "Completada"
        }).execute()

        # 🔍 Calcular estadísticas
        cleaned_preview = df.head(10).to_dict(orient="records")
        stats = {
            "total_filas": int(len(df)),
            "total_columnas": int(len(df.columns)),
            "nulls": int(df.isnull().sum().sum()),
            "duplicados": int(df.duplicated().sum()),
            "outliers": 0  # solo se detectan si se aplicó limpieza específica
        }

        return jsonify({
            "message": "Limpieza completada correctamente.",
            "limpieza_id": limpieza_insert.data[0]["id"],
            "ruta_dataset_limpio": ruta_storage_clean,
            "total_afectados": int(total_afectados),
            "operaciones": operaciones_realizadas,
            "cleaned_preview": cleaned_preview,
            **stats
        }), 200

    except Exception as e:
        print("❌ Error en /api/limpiezas:", e)
        return jsonify({"error": str(e)}), 500

# ------------------------------------------------------------
# 🚀 3. ENTRENAMIENTOS
# ------------------------------------------------------------
@app.route('/api/entrenamientos', methods=['POST'])
def create_entrenamiento():
    data = request.get_json()
    required_fields = ['limpieza_id', 'tipo_modelo']
    if not all(field in data for field in required_fields):
        return jsonify({"message": "Faltan limpieza_id y tipo_modelo."}), 400

    try:
        response = supabase.table("entrenamientos").insert({
            "limpieza_id": data['limpieza_id'],
            "tipo_modelo": data['tipo_modelo'],
            "epocas": data.get('epocas'),
            "batch_size": data.get('batch_size'),
            "learning_rate": data.get('learning_rate'),
            "operaciones_limpieza": data.get('operaciones_limpieza', []),
            "estado": 'En Curso',
            "fecha_inicio": datetime.datetime.now().isoformat()
        }).execute()

        return jsonify({
            "message": "Entrenamiento registrado exitosamente.",
            "entrenamiento_id": response.data[0]['id'],
            "estado": response.data[0]['estado']
        }), 201

    except Exception as e:
        return jsonify({"message": f"Error al registrar entrenamiento: {str(e)}"}), 500


# ------------------------------------------------------------
# 🚀 4. RESULTADOS
# ------------------------------------------------------------
@app.route('/api/resultados', methods=['POST'])
def create_resultado():
    data = request.get_json()
    required = ['entrenamiento_id', 'accuracy', 'f1_score', 'loss_final']
    if not all(field in data for field in required):
        return jsonify({"message": "Faltan campos requeridos."}), 400

    try:
        res = supabase.table("resultados_metricas").insert({
            "entrenamiento_id": data['entrenamiento_id'],
            "accuracy": data['accuracy'],
            "f1_score": data['f1_score'],
            "loss_final": data['loss_final'],
            "grafico_accuracy_f1": data.get('grafico_accuracy_f1'),
            "grafico_loss": data.get('grafico_loss'),
            "modelo_guardado": data.get('modelo_guardado')
        }).execute()

        supabase.table("entrenamientos").update({
            "estado": "Finalizado",
            "fecha_fin": datetime.datetime.now().isoformat()
        }).eq("id", data['entrenamiento_id']).execute()

        return jsonify({
            "message": "Resultados guardados y entrenamiento finalizado.",
            "resultado_id": res.data[0]['id']
        }), 201

    except Exception as e:
        return jsonify({"message": f"Error al guardar resultados: {str(e)}"}), 500


# ------------------------------------------------------------
# 🚀 INICIO
# ------------------------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
