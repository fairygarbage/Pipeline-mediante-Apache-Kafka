from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def plot_data(mongo_uri):
    client = MongoClient(mongo_uri)
    db = client['data_pipeline']
    
    weather_data = list(db['weather'].find())
    aqicn_data = list(db['aqicn'].find())
    
    if not weather_data or not aqicn_data:
        print("No data found in MongoDB")
        return
    
    # Procesar datos del clima
    temperatures_min = [item['temp_min'] for item in weather_data]
    temperatures_max = [item['temp_max'] for item in weather_data]
    cities_weather = [item['city'] for item in weather_data]
    
    # Procesar datos de calidad del aire
    aqi = [item['aqi'] for item in aqicn_data]
    cities_aqicn = [item['city'] for item in aqicn_data]
    
    # Crear el DataFrame para la gráfica de AQI
    aqi_df = pd.DataFrame({
        'City': cities_aqicn,
        'AQI': aqi
    })
    
    # Asignar colores en función del valor de AQI
    def get_aqi_color(aqi_value):
        if 0 <= aqi_value <= 50:
            return 'green'
        elif 51 <= aqi_value <= 100:
            return 'yellow'
        elif 101 <= aqi_value <= 150:
            return 'orange'
        elif 151 <= aqi_value <= 200:
            return 'red'
        else:
            return 'purple'
    
    aqi_df['Color'] = aqi_df['AQI'].apply(get_aqi_color)
    
    # Gráfica de temperaturas en una ventana separada
    plt.figure(figsize=(15, 7))

    # Gráfica de temperaturas mínimas
    plt.subplot(1, 2, 1)
    sns.barplot(x=cities_weather, y=temperatures_min)
    plt.xlabel('City')
    plt.ylabel('Temp Min (°C)')
    plt.title('Min Temperature by City')
    plt.xticks(rotation=45, ha='right')

    # Gráfica de temperaturas máximas
    plt.subplot(1, 2, 2)
    sns.barplot(x=cities_weather, y=temperatures_max)
    plt.xlabel('City')
    plt.ylabel('Temp Max (°C)')
    plt.title('Max Temperature by City')
    plt.xticks(rotation=45, ha='right')

    plt.tight_layout()
    plt.show()

    # Gráfica de AQI en una ventana separada
    plt.figure(figsize=(15, 7))

    bars = plt.bar(aqi_df['City'], aqi_df['AQI'], color=aqi_df['Color'])
    plt.xlabel('City')
    plt.ylabel('Air Quality Index (AQI)')
    plt.title('Air Quality Index by City')
    plt.xticks(rotation=45, ha='right')  # Rotar etiquetas de eje X
    
    # Añadir leyenda
    from matplotlib.patches import Patch
    legend_colors = [
        Patch(color='green', label='0-50 (Good)'),
        Patch(color='yellow', label='51-100 (Moderate)'),
        Patch(color='orange', label='101-150 (Unhealthy for Sensitive Groups)'),
        Patch(color='red', label='151-200 (Unhealthy)'),
        Patch(color='purple', label='>200 (Very Unhealthy)')
    ]
    plt.legend(handles=legend_colors, title='AQI Levels')

    plt.tight_layout()
    plt.show()


#Main
mongo_uri = "mongodb+srv://a345635:123@pipeline-info.tdukogc.mongodb.net/"
print("Visualizando los datos almacenados en MongoDB...")
plot_data(mongo_uri)
