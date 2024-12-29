import streamlit as st
from mysql import connector
import pandas as pd

def get_connection():
    connection = connector.connect(
    host = "localhost",
    user = "root",
    password = "root",
    database = 'com_guvi_redbus'
    )
    return connection


def fetch_statename(connection):
    query = """
    select DISTINCT State
    from redbus
    ORDER BY State
    """
    my_cursor = connection.cursor()
    my_cursor.execute(query)
    result = my_cursor.fetchall()
    State_names = [row[0] for row in result]
    return State_names

def fetch_bus_types(connection):
    query = """
    select DISTINCT Bus_Type
    from redbus
    ORDER BY Bus_Type
    """
    my_cursor = connection.cursor()
    my_cursor.execute(query)
    result = my_cursor.fetchall()
    bus_types = [row[0] for row in result]
    return bus_types

def fetch_data(connection, state, route_name, price_range, star_range, bus_type, availability, price_sort_order, depature_slot):
    price_sort_order_sql = "ASC" if price_sort_order == "low to high" else "DESC"
    depature_time_slot = {
        "6 AM - 12 PM" :("6:00", "12:00"),
        "12 PM - 6 PM" :("12:00", "18:00"),
        "6 PM - 12 AM" :("18:00", "23:59"),
        "12 AM - 6 AM" :("00:00", "6:00"),
    }
    start_time, end_time = depature_time_slot.get(depature_slot, ("00:00", "23:59"))
    bus_type_condition = ""
    if bus_type != "All":
        bus_type_condition = "AND Bus_Type LIKE %s"
    
    query = f"""
        select *
        from redbus
        where State = %s
            AND Route_Name = %s
            AND Price BETWEEN %s AND %s
            AND Star_Rating BETWEEN %s AND %s
            {bus_type_condition}
            AND(%s IS NULL OR Seat_Availability > 0)
            AND Departing_Time BETWEEN %s AND %s
            ORDER BY Star_Rating DESC, Price {price_sort_order_sql}
    """
   
    params=[
            state,
            route_name,
            price_range[0],
            price_range[1],
            star_range[0],
            star_range[1],
            bus_type if bus_type != "All" else None,
            None if availability else 0,
            start_time,
            end_time,
        ]
    params = [param for param in params if param is not None]

    my_cursor = connection.cursor()
    my_cursor.execute(query,tuple(params))
    result_data = my_cursor.fetchall()
    df = pd.DataFrame(result_data, columns=my_cursor.column_names)
    print(df)
    return df

# Main Streamlit app
def main():
    st.title("Book My Bus")

    st.balloons()
    st.image('https://seeklogo.com/images/R/redbus-logo-13648C0E43-seeklogo.com.png')

    connection =get_connection() 
    

    try:
        
        # fetch available state names
        states = fetch_statename(connection) 
        
        
        # user input for states 
        state = st.selectbox("select State:",states)
        
        if state:
            route_query = """
            select DISTINCT Route_Name
            from redbus
            where State = %s
            """
        my_cursor = connection.cursor()
        my_cursor.execute(route_query, (state,))
        result = my_cursor.fetchall()
        route_names = [row[0] for row in result]
        if route_names:
            route_name = st.selectbox("Select a route" , route_names)

            # Additional filters
            price_range = st.slider("Select Price Range:", 0, 5000, (500, 2000), step=50)
            star_range = st.slider("Select Star Rating Range:", 0.0, 5.0, (3.0, 4.5), step=0.5)
            bus_types = ["All"] + fetch_bus_types(connection)
            bus_type = st.selectbox("Select Bus Type:", bus_types)
            price_sort_order = st.radio("Sort by Price:", ["Low to High", "High to Low"])
            depature_slot = st.selectbox(
                        "Select Departure Time Slot:",
                        ["6 AM - 12 PM", "12 PM - 6 PM", "6 PM - 12 AM", "12 AM - 6 AM"]
                    )
            availability = st.write("Only show available buses")
            if st.button("show Bus"):
                bus_data = fetch_data(
                    connection, state, route_name, price_range, star_range, bus_type, availability, price_sort_order, depature_slot
                )
                if not bus_data.empty:
                    st.write("Available Buses:")
                    st.dataframe(bus_data)
                else:
                    st.warning("No buses found for the selected criteria.")

        else:
            st.warning("No routes found for the selected state.")
            
    except Exception as e:
        st.error(f"Error: {e}") 

if __name__ == '__main__':
    main()