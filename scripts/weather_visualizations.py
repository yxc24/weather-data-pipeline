import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import os
from matplotlib.backends.backend_pdf import PdfPages


def connect_to_db():
    conn = psycopg2.connect(
        dbname="weatherdb",
        user="USER_NAME",
        password="PASSWORD",
        host="localhost",
        port="5432"
    )
    return conn


def get_weather_data(conn):
    query = """
    SELECT city, temperature, humidity, wind_speed, weather_main, timestamp
    FROM weather_data
    WHERE country = 'AU' 
      AND city IN ('Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Hobart', 'Darwin')
      AND timestamp BETWEEN 
          (DATE_TRUNC('day', NOW()) - INTERVAL '1 day' + INTERVAL '6 hour') 
          AND 
          (DATE_TRUNC('day', NOW()) - INTERVAL '1 day' + INTERVAL '18 hour')
    ORDER BY city, timestamp
    """
    return pd.read_sql_query(query, conn)



def generate_visualizations(data):
    plt.rcParams.update({'font.size': 14})  # Increase the default font size even more

    fig, axs = plt.subplots(2, 2, figsize=(24, 24))
    fig.suptitle('Weather Data Visualization', fontsize=30)

    # Weather Main Pie Chart
    weather_main_counts = data['weather_main'].value_counts()
    axs[0, 0].pie(weather_main_counts.values, labels=weather_main_counts.index, autopct='%1.1f%%',
                  textprops={'fontsize': 14})
    axs[0, 0].set_title('Distribution of Weather Conditions', fontsize=24)

    # Temperature Line Chart
    for city in data['city'].unique():
        city_data = data[data['city'] == city]
        axs[0, 1].plot(city_data['timestamp'], city_data['temperature'], label=city)
    axs[0, 1].set_xlabel('Date', fontsize=18)
    axs[0, 1].set_ylabel('Temperature (°C)', fontsize=18)
    axs[0, 1].set_title('Temperature Over Time', fontsize=24)
    axs[0, 1].legend(fontsize=16)
    axs[0, 1].tick_params(axis='x', rotation=45, labelsize=14)
    axs[0, 1].tick_params(axis='y', labelsize=14)

    # Humidity vs Temperature Scatter Plot
    sns.scatterplot(data=data, x='temperature', y='humidity', hue='city', ax=axs[1, 0])
    axs[1, 0].set_xlabel('Temperature (°C)', fontsize=18)
    axs[1, 0].set_ylabel('Humidity (%)', fontsize=18)
    axs[1, 0].set_title('Humidity vs Temperature', fontsize=24)
    axs[1, 0].legend(fontsize=16)
    axs[1, 0].tick_params(axis='both', labelsize=16)

    # Wind Speed Box Plot
    sns.boxplot(data=data, x='city', y='wind_speed', ax=axs[1, 1])
    axs[1, 1].set_xlabel('City', fontsize=18)
    axs[1, 1].set_ylabel('Wind Speed (m/s)', fontsize=18)
    axs[1, 1].set_title('Wind Speed Distribution', fontsize=24)
    axs[1, 1].tick_params(axis='x', rotation=45, labelsize=14)
    axs[1, 1].tick_params(axis='y', labelsize=14)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])  # Adjust layout to prevent cutting off labels
    return fig


def save_visualizations(fig, output_dir):
    # Save as PNG
    png_path = os.path.join(output_dir, 'weather_visualizations.png')
    fig.savefig(png_path, dpi=300, bbox_inches='tight')  # Use tight bounding box

    # Save as PDF
    pdf_path = os.path.join(output_dir, 'weather_visualizations.pdf')
    with PdfPages(pdf_path) as pdf:
        pdf.savefig(fig, bbox_inches='tight')  # Use tight bounding box

    plt.close(fig)
    return png_path, pdf_path


def main():
    print("Starting visualization generation...")

    conn = connect_to_db()
    data = get_weather_data(conn)
    conn.close()

    fig = generate_visualizations(data)

    output_dir = "/weather-data-pipeline/weather_visualizations"
    os.makedirs(output_dir, exist_ok=True)

    png_path, pdf_path = save_visualizations(fig, output_dir)

    print(f"Visualizations saved as PNG: {png_path}")
    print(f"Visualizations saved as PDF: {pdf_path}")


if __name__ == "__main__":
    main()