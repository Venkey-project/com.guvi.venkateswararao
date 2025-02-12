import streamlit as st
import pandas as pd
import numpy as np
import datetime

def load_data():
    try:
        
        price_df = pd.read_csv(r"..\Data\Cleaned_Flight_Price.csv")
        satisfaction_df = pd.read_csv(r"..\Data\Cleaned_Passenger_Satisfaction.csv")
        
        # Print column names for debugging
        return price_df, satisfaction_df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None

def main():
    st.title("Flight Price and Customer Satisfaction Prediction")
    
    # Load datasets
    price_df, satisfaction_df = load_data()
    
    if price_df is None or satisfaction_df is None:
        st.error("eror")
        return
    
    # Create tabs for different predictions
    tab1, tab2 = st.tabs(["Flight Price Prediction", "Customer Satisfaction Prediction"])
    
    with tab1:
        st.header("Flight Price Prediction")
        
        # Create columns for better layout
        col1, col2 = st.columns(2)
        
        with col1:
            if 'Total_Stops' in price_df.columns:
                total_stops = st.selectbox("Number of Stops", 
                                         options=sorted(price_df['Total_Stops'].unique()))
            
            if 'Airline' in price_df.columns:
                airline = st.selectbox("Airline", 
                                     options=sorted(price_df['Airline'].unique()))
            
            if 'Source' in price_df.columns:
                source = st.selectbox("Source", 
                                    options=sorted(price_df['Source'].unique()))
            
            if 'Destination' in price_df.columns:
                destination = st.selectbox("Destination", 
                                         options=sorted(price_df['Destination'].unique()))
        
        with col2:
            # Date and time inputs
            date = st.date_input("Date of Journey", 
                               min_value=datetime.date(2019, 1, 1))
            dep_time = st.time_input("Departure Time")
            duration = st.number_input("Duration (minutes)", 
                                     min_value=0, max_value=1440)
        
        if st.button("Predict Price"):
            try:
                # Basic prediction using available features
                base_price = 5000  # Default base price
                
                # Adjust price based on available features
                if 'Total_Stops' in locals():
                    base_price += total_stops * 1000
                
                if 'duration' in locals():
                    base_price += duration * 2
                
                time_factor = 1 + (dep_time.hour - 12) * 0.02
                price_prediction = base_price * time_factor
                
                st.success(f"Predicted Flight Price: ₹{price_prediction:,.2f}")
                
            except Exception as e:
                st.error(f"Error in prediction: {e}")
    
    with tab2:
        st.header("Customer Satisfaction Prediction")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'Age' in satisfaction_df.columns:
                age = st.number_input("Age", 
                                    min_value=int(satisfaction_df['Age'].min()),
                                    max_value=int(satisfaction_df['Age'].max()),
                                    value=30)
            
            if 'Flight Distance' in satisfaction_df.columns:
                flight_distance = st.number_input("Flight Distance",
                                               min_value=int(satisfaction_df['Flight Distance'].min()),
                                               max_value=int(satisfaction_df['Flight Distance'].max()),
                                               value=500)
            
            wifi_service = st.slider("Inflight Wifi Service", 0, 5, 3)
            online_booking = st.slider("Ease of Online Booking", 0, 5, 3)
            
        with col2:
            food_drink = st.slider("Food and Drink", 0, 5, 3)
            seat_comfort = st.slider("Seat Comfort", 0, 5, 3)
            entertainment = st.slider("Inflight Entertainment", 0, 5, 3)
            on_board_service = st.slider("On-board Service", 0, 5, 3)
            
        with col3:
            cleanliness = st.slider("Cleanliness", 0, 5, 3)
            departure_delay = st.number_input("Departure Delay (minutes)", 
                                           min_value=0,
                                           value=0)
            arrival_delay = st.number_input("Arrival Delay (minutes)", 
                                         min_value=0,
                                         value=0)
            
            if 'Type of Travel' in satisfaction_df.columns:
                travel_type = st.selectbox("Type of Travel", 
                                         options=satisfaction_df['Type of Travel'].unique())
            
        if st.button("Predict Satisfaction"):
            try:
                service_score = (wifi_service + online_booking + food_drink + 
                               seat_comfort + entertainment + on_board_service + 
                               cleanliness) / 35
                
                delay_impact = (departure_delay + arrival_delay) / 1000
                
                final_satisfaction_score = service_score - delay_impact
                
                satisfaction_prediction = 1 if final_satisfaction_score > 0.6 else 0
                result = "Satisfied" if satisfaction_prediction == 1 else "Not Satisfied"
                confidence = abs(final_satisfaction_score - 0.6) * 100
                
                st.success(f"Predicted Customer Satisfaction: {result}")
                st.info(f"Prediction Confidence: {confidence:.1f}%")
                
            except Exception as e:
                st.error(f"Error in prediction: {e}")

if __name__ == "__main__":
    st.set_page_config(page_title="Flight Predictions", layout="wide")
    main()