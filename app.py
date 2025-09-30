import streamlit as st
import pyodbc
import pandas as pd
import os
from datetime import datetime
from io import StringIO
import traceback


class SQLServerConnection:
    """Handles Microsoft SQL Server database connections and operations."""

    def __init__(self):
        self.connection = None
        self.is_connected = False

    def connect(self, server, database, auth_type, username=None, password=None):
        """
        Connect to SQL Server database.

        Args:
            server (str): SQL Server instance name
            database (str): Database name
            auth_type (str): 'Windows' or 'SQL Server'
            username (str): Username for SQL Server auth (optional)
            password (str): Password for SQL Server auth (optional)

        Returns:
            tuple: (success: bool, message: str)
        """
        try:
            if auth_type == "Windows":
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"Trusted_Connection=yes;"
                )
            else:
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"UID={username};"
                    f"PWD={password};"
                )

            self.connection = pyodbc.connect(connection_string, timeout=10)
            self.is_connected = True
            return True, "Connected successfully!"

        except pyodbc.Error as e:
            self.is_connected = False
            return False, f"Connection failed: {str(e)}"
        except Exception as e:
            self.is_connected = False
            return False, f"Unexpected error: {str(e)}"

    def disconnect(self):
        """Close the database connection."""
        if self.connection:
            try:
                self.connection.close()
                self.is_connected = False
                return True, "Disconnected successfully"
            except Exception as e:
                return False, f"Error disconnecting: {str(e)}"
        return True, "No active connection"

    def execute_stored_procedure(self, procedure_name, parameters=None):
        """
        Execute a stored procedure with parameters.

        Args:
            procedure_name (str): Name of the stored procedure
            parameters (dict): Dictionary of parameter names and values

        Returns:
            tuple: (success: bool, data: DataFrame or None, message: str)
        """
        if not self.is_connected or not self.connection:
            return False, None, "No active database connection"

        try:
            cursor = self.connection.cursor()

            if parameters:
                param_placeholders = ', '.join(['?' for _ in parameters])
                param_values = list(parameters.values())
                query = f"EXEC {procedure_name} {param_placeholders}"
                cursor.execute(query, param_values)
            else:
                query = f"EXEC {procedure_name}"
                cursor.execute(query)

            columns = [column[0] for column in cursor.description] if cursor.description else []
            rows = cursor.fetchall()

            if columns and rows:
                df = pd.DataFrame.from_records(rows, columns=columns)
                return True, df, f"Procedure executed successfully. {len(rows)} rows returned."
            else:
                return True, pd.DataFrame(), "Procedure executed successfully. No data returned."

        except pyodbc.Error as e:
            return False, None, f"SQL Error: {str(e)}"
        except Exception as e:
            return False, None, f"Unexpected error: {str(e)}"


def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if 'db_connection' not in st.session_state:
        st.session_state.db_connection = SQLServerConnection()
    if 'connection_status' not in st.session_state:
        st.session_state.connection_status = None
    if 'last_result' not in st.session_state:
        st.session_state.last_result = None
    if 'procedure_parameters' not in st.session_state:
        st.session_state.procedure_parameters = {}
    if 'show_connection_area' not in st.session_state:
        st.session_state.show_connection_area = True
    if 'stored_connection_info' not in st.session_state:
        st.session_state.stored_connection_info = {
            'server': '',
            'database': '',
            'auth_type': 'Windows',
            'username': ''
        }
    if 'password_key' not in st.session_state:
        st.session_state.password_key = 0


def render_sidebar():
    """Render the sidebar for database connection settings."""

    # Connection status and toggle button at the top
    if st.session_state.db_connection.is_connected:
        col1, col2 = st.sidebar.columns([3, 1])
        with col1:
            st.sidebar.success("✅ Connected to Database")
        with col2:
            if st.sidebar.button("⚙️"):
                st.session_state.show_connection_area = not st.session_state.show_connection_area
    else:
        st.sidebar.header("🗄️ Connect To SQL Server")

    # Show connection area based on toggle state or connection status
    if st.session_state.show_connection_area or not st.session_state.db_connection.is_connected:

        if st.session_state.db_connection.is_connected:
            st.sidebar.markdown("---")
            st.sidebar.subheader("SQL Server Connection Settings")
            # When connected and showing settings, display stored values but keep password hidden
            server = st.sidebar.text_input(
                "Server Name",
                value=st.session_state.stored_connection_info['server'],
                placeholder="e.g., localhost or server\\instance",
                disabled=True
            )

            database = st.sidebar.text_input(
                "Database Name",
                value=st.session_state.stored_connection_info['database'],
                placeholder="e.g., MyDatabase",
                disabled=True
            )

            auth_type = st.sidebar.selectbox(
                "Authentication Type",
                ["Windows", "SQL Server"],
                index=0 if st.session_state.stored_connection_info['auth_type'] == 'Windows' else 1,
                disabled=True
            )

            if st.session_state.stored_connection_info['auth_type'] == "SQL Server":
                st.sidebar.text_input(
                    "Username",
                    value=st.session_state.stored_connection_info['username'],
                    disabled=True
                )
                # Display a simple text indicating password is hidden
                st.sidebar.markdown("**Password:** `••••••••` *(hidden for security)*")

        else:
            # When not connected, show normal editable form with previous values (if any)
            server = st.sidebar.text_input(
                "Server Name",
                value=st.session_state.stored_connection_info['server'] if st.session_state.stored_connection_info['server'] else "localhost",
                placeholder="e.g., localhost or server\\instance"
            )

            database = st.sidebar.text_input(
                "Database Name",
                value=st.session_state.stored_connection_info['database'],
                placeholder="e.g., MyDatabase"
            )

            auth_type = st.sidebar.selectbox(
                "Authentication Type",
                ["Windows", "SQL Server"],
                index=0 if st.session_state.stored_connection_info['auth_type'] == 'Windows' else 1
            )

        username = None
        password = None

        # Only allow input when not connected
        if not st.session_state.db_connection.is_connected and auth_type == "SQL Server":
            username = st.sidebar.text_input(
                "Username",
                value=st.session_state.stored_connection_info['username']
            )
            password = st.sidebar.text_input("Password", type="password", key=f"password_{st.session_state.password_key}")


        # Button styling CSS
        if not st.session_state.db_connection.is_connected:
            # When not connected - only Connect button (green)
            st.markdown("""
            <style>
            .stButton > button {
                white-space: nowrap;
                width: 100%;
                background-color: #28a745 !important;
                color: white !important;
                border: 1px solid #28a745 !important;
            }
            .stButton > button:hover {
                background-color: #218838 !important;
                border-color: #1e7e34 !important;
            }
            </style>
            """, unsafe_allow_html=True)
        else:
            # When connected - only Disconnect button (red)
            st.markdown("""
            <style>
            .stButton > button {
                white-space: nowrap;
                width: 100%;
                background-color: #dc3545 !important;
                color: white !important;
                border: 1px solid #dc3545 !important;
            }
            .stButton > button:hover {
                background-color: #c82333 !important;
                border-color: #bd2130 !important;
            }
            </style>
            """, unsafe_allow_html=True)

        col1, col2 = st.sidebar.columns([1, 1])

        with col1:
            if not st.session_state.db_connection.is_connected:
                connect_clicked = st.button("Connect")
                if connect_clicked:
                    if not server or not database:
                        st.sidebar.error("Please provide server and database names")
                    elif auth_type == "SQL Server" and (not username or not password):
                        st.sidebar.error("Please provide username and password")
                    else:
                        with st.spinner("Connecting..."):
                            success, message = st.session_state.db_connection.connect(
                                server, database, auth_type, username, password
                            )
                            st.session_state.connection_status = (success, message)
                            if success:
                                st.session_state.stored_connection_info = {
                                    'server': server,
                                    'database': database,
                                    'auth_type': auth_type,
                                    'username': username if username else ''
                                }
                                st.session_state.password_key += 1
                                st.rerun()

        with col2:
            if st.session_state.db_connection.is_connected:
                disconnect_clicked = st.button("Disconnect")
                if disconnect_clicked:
                    success, message = st.session_state.db_connection.disconnect()
                    st.session_state.connection_status = (False, message)
                    st.session_state.last_result = None
                    st.rerun()

        # Show detailed connection status messages
        if st.session_state.connection_status:
            success, message = st.session_state.connection_status
            if success:
                st.sidebar.success(f"✅ {message}")
            else:
                st.sidebar.error(f"❌ {message}")

    elif st.session_state.db_connection.is_connected:
        # Show minimal info when connection area is hidden
        st.sidebar.markdown("*Connection details hidden*")
        st.sidebar.markdown("*Click ⚙️ to show settings*")


def render_procedure_interface():
    """Render the main interface for stored procedure execution."""
    st.header("📊 Stored Procedure Execution")

    if not st.session_state.db_connection.is_connected:
        st.warning("⚠️ Please connect to a database first using the sidebar.")
        return

    procedure_name = st.text_input(
        "Stored Procedure Name",
        placeholder="e.g., sp_GetEmployees"
    )

    st.subheader("Parameters")
    col1, col2 = st.columns([1, 3])

    with col1:
        num_params = st.number_input(
            "Number of Parameters",
            min_value=0,
            max_value=20,
            value=0
        )

    parameters = {}
    if num_params > 0:
        st.write("Enter parameter details:")
        for i in range(num_params):
            col1, col2 = st.columns(2)
            with col1:
                param_name = st.text_input(f"Parameter {i+1} Name", key=f"param_name_{i}")
            with col2:
                param_value = st.text_input(f"Parameter {i+1} Value", key=f"param_value_{i}")

            if param_name and param_value:
                parameters[param_name] = param_value

    col1, col2, col3 = st.columns([1, 1, 2])

    with col1:
        if st.button("Execute Procedure", type="primary"):
            if not procedure_name:
                st.error("Please enter a stored procedure name")
            else:
                with st.spinner("Executing stored procedure..."):
                    success, data, message = st.session_state.db_connection.execute_stored_procedure(
                        procedure_name, parameters if parameters else None
                    )

                    if success:
                        st.success(message)
                        st.session_state.last_result = data
                    else:
                        st.error(message)
                        st.session_state.last_result = None


def render_results_and_export():
    """Render results display and CSV export functionality."""
    if st.session_state.last_result is not None:
        st.header("📋 Results")

        data = st.session_state.last_result

        if not data.empty:
            st.dataframe(data, use_container_width=True)

            st.subheader("📥 Export to CSV")

            col1, col2 = st.columns(2)

            with col1:
                include_timestamp = st.checkbox(
                    "Include timestamp in filename",
                    value=True
                )

                base_filename = st.text_input(
                    "Filename (without extension)",
                    value="stored_procedure_results"
                )

            with col2:
                if include_timestamp:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    full_filename = f"{base_filename}_{timestamp}.csv"
                else:
                    full_filename = f"{base_filename}.csv"

                st.text_input("Full filename:", value=full_filename, disabled=True)

            csv_data = data.to_csv(index=False)

            col1, col2 = st.columns(2)

            with col1:
                st.download_button(
                    label="📱 Download CSV",
                    data=csv_data,
                    file_name=full_filename,
                    mime="text/csv",
                    type="primary"
                )

            with col2:
                if st.button("📋 Copy to Clipboard"):
                    st.code(csv_data, language="csv")
                    st.info("CSV data displayed above - copy manually")

            with st.expander("Preview CSV Data"):
                st.text(csv_data[:1000] + "..." if len(csv_data) > 1000 else csv_data)

        else:
            st.info("No data returned from the stored procedure.")
    else:
        st.info("Execute a stored procedure to see results here.")


def main():
    """Main Streamlit application."""
    st.set_page_config(
        page_title="QuikSQL Connect - Streamlit App",
        page_icon="🔗",
        layout="wide"
    )


    st.title("🔗 Cypress Exposure File creation")
    st.markdown("Execute stored procedures and export results to CSV files")

    initialize_session_state()

    render_sidebar()

    tab1, tab2 = st.tabs(["🚀 Execute Procedures", "📊 Results & Export"])

    with tab1:
        render_procedure_interface()

    with tab2:
        render_results_and_export()

    st.sidebar.markdown("---")
    st.sidebar.markdown("© Cypress 2025")


if __name__ == "__main__":
    main()