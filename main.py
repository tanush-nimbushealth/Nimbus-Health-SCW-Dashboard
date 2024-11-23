import requests
import pandas as pd
import json
import plotly.express as px
from dash import Dash, html, dcc, Input, Output
import pytz
import plotly.graph_objects as go
import dash_table
import time
from datetime import datetime, timedelta

# Constants
API_KEY = "86muUmwYga8D2yUfuVX3dCsgPWRLwvkP7S7UXUnbvw2fV3NrzMv8EfFR35s9a9XHSmTjfgnzbGSDTCmLrqTWLmD6jYRZgepw4rEb"
BASE_URL = "https://dialpad.com/api/v2/stats"
LOOKBACK_OPTIONS = [
    {'label': 'Last 7 Days', 'value': 7},
    {'label': 'Last 14 Days', 'value': 14},
    {'label': 'Last 30 Days', 'value': 30},
    {'label': 'Last 60 Days', 'value': 60},
    {'label': 'Last 90 Days', 'value': 90},
]

# Add column name mapping configuration
COLUMN_MAPPING = {
    'call_id': 'master_call_id',          # Unique identifier for each call
    'start_time': 'date_started',         # When the call started
    'connect_time': 'date_connected',     # When the call was answered (if it was)
    'group': 'group',                     # Group/department information
    'user_id': 'target_id'                # Added: User identifier
    # voicemail and direction will stay as is since they're standard names
}

url = "https://dialpad.com/api/v2/stats?apikey=86muUmwYga8D2yUfuVX3dCsgPWRLwvkP7S7UXUnbvw2fV3NrzMv8EfFR35s9a9XHSmTjfgnzbGSDTCmLrqTWLmD6jYRZgepw4rEb"

payload = json.dumps({"export_type": "records", "stat_type": "calls", "days_ago_end": 7, "days_ago_start": 120, "group_by": "group", "office_id": "5874942103666688", "timezone": "UTC"})

headers = {'Content-Type': 'application/json','Accept': 'application/json'}

job_id = requests.request("POST", url, headers=headers, data=payload)

request_id = job_id.json()['request_id']

print(job_id.text)

# Get job results

url = f'https://dialpad.com/api/v2/stats/{request_id}?apikey=86muUmwYga8D2yUfuVX3dCsgPWRLwvkP7S7UXUnbvw2fV3NrzMv8EfFR35s9a9XHSmTjfgnzbGSDTCmLrqTWLmD6jYRZgepw4rEb'

response = requests.request("GET", url, headers={'Accept': 'application/json'}, data={})

print(response.text)

call_data = pd.read_csv(response.json()['download_url'])

# Add this constant near the top of the file with other constants
USER_MAPPING = {
    '5260830834245632': 'Sarah Davis',
    '4777918533877760': 'Jeff Savage',
    '5437478106710016': 'Shannon Valenzuela',
    '4739483989065728': 'Nimbus at Home Andrea Sleep Tech',
    '5391378276564992': 'Don',
    '6056416280723456': 'Craig Rundbaken',
    '6151466557784064': 'Backline',
    '6719772188884992': 'Janice Go',
    '6174430947590144': 'SunCity West',
    '6026899252199424': 'Brenda MA',
    '5534215803322368': 'Medical Assistant Valenzuela',
    '5126951769882624': 'Front Desk Check In',
    '5385579755421696': 'Front Desk Scheduler',
    '4633012824588288': 'Sarah Davis',
    '6551734434807808': 'Cheyenne Fagan',
    '5760839349321728': 'Front Desk Check Out',
    '6499526680920064': 'Medical Assistant Rundbaken 1',
    '5827897546129408': 'Practice Manager',
    '4577241925566464': 'Medical Assistant Rundbaken 2',
    '4821760453525504': 'Front Office Lead'
}

# Add this near the top with other constants
DEPARTMENT_MAPPING = {
    '6151466557784064': 'Backline',
    '6719772188884992': 'Nimbus Sun City West',
    '6174430947590144': 'Nimbus Sun City West',
    '6026899252199424': 'Medical',
    '5534215803322368': 'Medical',
    '5126951769882624': 'Front Desk',
    '5385579755421696': 'Front Desk',
    '4633012824588288': 'NAH',
    '6551734434807808': 'NAH',
    '5760839349321728': 'Billing,Front Desk',
    '6499526680920064': 'Just Maria,Medical',
    '5827897546129408': 'Office Manager,Billing,Doctor or Hospital Calls',
    '4577241925566464': 'Just Trish,Just Maria,Medical',
    '4821760453525504': 'Billing,Front Desk,Doctor or Hospital Calls'
}

# Get unique department names from the values
DEPARTMENTS = sorted(list(set(
    dept.strip()
    for depts in DEPARTMENT_MAPPING.values()
    for dept in depts.split(',')
)))

# Create dropdown options with actual department names
DEPARTMENT_OPTIONS = [{'label': 'All Departments', 'value': 'all'}] + [
    {'label': dept, 'value': dept} for dept in DEPARTMENTS
]

# Add this helper function
def get_user_name(user_id):
    """Convert user ID to friendly name, falling back to ID if not mapped"""
    return USER_MAPPING.get(str(user_id), str(user_id))

# Function to process call data
def process_call_data(call_data):
    # Convert the start time to datetime and localize to Arizona timezone
    call_data[COLUMN_MAPPING['start_time']] = pd.to_datetime(call_data[COLUMN_MAPPING['start_time']])
    utc = pytz.utc
    arizona = pytz.timezone('US/Arizona')
    call_data[COLUMN_MAPPING['start_time']] = (
        call_data[COLUMN_MAPPING['start_time']]
        .dt.tz_localize(utc)
        .dt.tz_convert(arizona)
    )
    
    # Extract hour and day of the week from the start time
    call_data['hour'] = call_data[COLUMN_MAPPING['start_time']].dt.hour
    call_data['day_of_week'] = call_data[COLUMN_MAPPING['start_time']].dt.day_name()
    
    # Filter calls to include only those between 8 AM and 5 PM
    call_data = call_data[call_data['hour'].between(8, 17)]
    
    return call_data

# Process the loaded call data
processed_data = process_call_data(call_data)

# Function to create a heatmap of call volume
def create_call_heatmap(call_data):
    # Get the actual date range from the data
    start_date = call_data[COLUMN_MAPPING['start_time']].min()
    end_date = call_data[COLUMN_MAPPING['start_time']].max()
    date_range = f"{start_date.strftime('%m/%d/%Y')} - {end_date.strftime('%m/%d/%Y')}"

    # Set the title for the heatmap
    title = (
        f"Call Volume Heatmap<br>"
        f"({date_range})"
    )

    # Prepare data for the heatmap
    call_data['date'] = call_data[COLUMN_MAPPING['start_time']].dt.date
    call_volume = call_data.groupby(['date', 'day_of_week', 'hour'])[COLUMN_MAPPING['call_id']].count().reset_index()
    daily_avg_call_volume = call_volume.groupby(['day_of_week', 'hour'])['master_call_id'].mean().reset_index().round(1)
    hour_labels = {h: f"{h-12 if h > 12 else h}{'PM' if h >= 12 else 'AM'}" for h in range(8, 18)}
    call_volume_pivot = daily_avg_call_volume.pivot(index='hour', columns='day_of_week', values='master_call_id').reindex(columns=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'])
    call_volume_pivot = call_volume_pivot.fillna(0)

    # Create the heatmap using Plotly
    fig = px.imshow(
        call_volume_pivot,
        labels=dict(x="Day of Week", y="Hour of Day", color="Average Number of Calls"),
        title=title,
        color_continuous_scale="Blues",
        text_auto=True
    )
    fig.update_layout(
        xaxis_title="Day of Week",
        yaxis_title="Hour of Day",
        xaxis=dict(tickfont=dict(size=16)),
        yaxis=dict(ticktext=[hour_labels[h] for h in range(8, 18)], tickvals=list(range(8, 18)), tickfont=dict(size=14))
    )
    return fig

# Function to create a heatmap of pick-up rates
def create_pickup_heatmap(call_data):
    # Get the actual date range from the data
    start_date = call_data[COLUMN_MAPPING['start_time']].min()
    end_date = call_data[COLUMN_MAPPING['start_time']].max()
    date_range = f"{start_date.strftime('%m/%d/%Y')} - {end_date.strftime('%m/%d/%Y')}"

    # Set the title for the heatmap
    title = (
        f"Pickup Time Heatmap<br>"
        f"({date_range})"
    )

    # Prepare data for the heatmap
    call_data['was_answered'] = call_data[COLUMN_MAPPING['connect_time']].notna().astype(int)
    pickup_stats = call_data.groupby(['day_of_week', 'hour']).agg({'was_answered': ['sum', 'count']}).reset_index()
    pickup_stats['pickup_rate'] = (pickup_stats[('was_answered', 'sum')] / pickup_stats[('was_answered', 'count')] * 100).round(1)
    hour_labels = {h: f"{h-12 if h > 12 else h}{'PM' if h >= 12 else 'AM'}" for h in range(8, 18)}
    pickup_pivot = pickup_stats.pivot(index='hour', columns='day_of_week', values='pickup_rate').reindex(columns=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'])
    pickup_pivot = pickup_pivot.fillna(0)

    # Create the heatmap using Plotly
    fig = px.imshow(
        pickup_pivot,
        labels=dict(x="Day of Week", y="Hour of Day", color="Pick-up Rate (%)"),
        title=title,
        color_continuous_scale="Blues",
        text_auto=True
    )
    fig.update_layout(
        xaxis_title="Day of Week",
        yaxis_title="Hour of Day",
        xaxis=dict(tickfont=dict(size=16)),
        yaxis=dict(ticktext=[hour_labels[h] for h in range(8, 18)], tickvals=list(range(8, 18)), tickfont=dict(size=14))
    )
    return fig

def fetch_call_data(selected_days=None, max_retries=5, retry_delay=3):
    """
    Fetch call data from the API with polling for completion and better error handling
    """
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    try:
        # Construct the payload
        payload = json.dumps({
            "export_type": "records",
            "stat_type": "calls",
            "days_ago_end": 0,
            "days_ago_start": selected_days if selected_days is not None else 120,
            "group_by": "group",
            "office_id": "5874942103666688",
            "timezone": "UTC"
        })
        
        print("\nStep 1: Making initial API request...")
        response = requests.post(f"{BASE_URL}?apikey={API_KEY}", headers=headers, data=payload)
        response.raise_for_status()
        
        initial_response = response.json()
        if 'request_id' not in initial_response:
            print(f"Error: No request_id in response: {initial_response}")
            return pd.DataFrame()
            
        request_id = initial_response['request_id']
        print(f"Got request_id: {request_id}")
        
        # Poll for results
        result_url = f"{BASE_URL}/{request_id}?apikey={API_KEY}"
        for attempt in range(max_retries):
            print(f"\nAttempt {attempt + 1}/{max_retries} to fetch results...")
            time.sleep(retry_delay)  # Wait between attempts
            
            result_response = requests.get(result_url, headers=headers)
            result_response.raise_for_status()
            
            result_json = result_response.json()
            print(f"Response status: {result_json.get('status', 'unknown')}")
            
            # Check if job is complete
            if result_json.get('status') == 'complete':
                if 'download_url' in result_json:
                    print("\nProcessing complete! Downloading CSV data...")
                    try:
                        call_data = pd.read_csv(result_json['download_url'])
                        if not call_data.empty:
                            print(f"Successfully loaded {len(call_data)} rows of data")
                            return call_data
                        else:
                            print("Error: Downloaded CSV is empty")
                    except Exception as e:
                        print(f"Error downloading or reading CSV: {str(e)}")
                else:
                    print(f"Error: No download_url in complete response: {result_json}")
            elif result_json.get('status') == 'failed':
                print(f"Job failed: {result_json.get('error', 'Unknown error')}")
                break
            elif result_json.get('status') == 'processing':
                print(f"Still processing... waiting {retry_delay} seconds")
            else:
                print(f"Unknown status: {result_json.get('status', 'unknown')}")
        
        print("Error: Maximum retries reached or job failed")
        return pd.DataFrame()
        
    except requests.exceptions.RequestException as e:
        print(f"Network error: {str(e)}")
        if hasattr(e.response, 'text'):
            print(f"Error response content: {e.response.text}")
        return pd.DataFrame()
        
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {str(e)}")
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        print(f"Error type: {type(e)}")
        return pd.DataFrame()

# Add new constant for VM response windows
VM_RESPONSE_WINDOWS = [
    {'label': '24 Hours', 'value': 24},
    {'label': '48 Hours', 'value': 48},
    {'label': '72 Hours', 'value': 72},
    {'label': '1 Week', 'value': 168},  # 24 * 7 = 168 hours
    {'label': '2 Weeks', 'value': 336}  # 24 * 14 = 336 hours
]

# Initialize the Dash app
app = Dash(__name__)
server = app.server

# Define the layout of the Dash app
app.layout = html.Div([
    dcc.Store(id='store-data'),
    dcc.Interval(id='interval-component', interval=300000),  # Updates every 5 minutes
    html.H1("Nimbus Health - Sun City West Dashboard", style={'text-align': 'center'}),
    dcc.Tabs([
        dcc.Tab(label='Call Volume Heatmap', children=[
            html.Div([
                dcc.Dropdown(
                    id='lookback-dropdown',
                    options=LOOKBACK_OPTIONS,
                    value=7,
                    style={'width': '200px', 'margin': '10px'}
                ),
                dcc.Graph(id='volume-heatmap')
            ])
        ]),
        dcc.Tab(label='Pick-up Rate Heatmap', children=[
            html.Div([
                dcc.Dropdown(
                    id='lookback-dropdown-2',  # New ID for second dropdown
                    options=LOOKBACK_OPTIONS,
                    value=7,
                    style={'width': '200px', 'margin': '10px'}
                ),
                dcc.Graph(id='pickup-heatmap')
            ])
        ]),
        dcc.Tab(label='Voicemail Response Rates', children=[
            html.Div([
                html.Div([
                    dcc.Dropdown(
                        id='vm-window-dropdown',
                        options=VM_RESPONSE_WINDOWS,
                        value=24,
                        style={'width': '200px', 'margin': '10px'}
                    ),
                    dcc.Dropdown(
                        id='department-dropdown',
                        options=DEPARTMENT_OPTIONS,
                        value='all',
                        style={'width': '200px', 'margin': '10px'}
                    ),
                ], style={'display': 'flex', 'justifyContent': 'flex-start'}),
                
                # Active responders graph
                dcc.Graph(
                    id='vm-response-chart',
                    style={'height': '800px'}
                ),
                
                # Non-responders graph
                dcc.Graph(
                    id='vm-nonresponse-chart',
                    style={'height': '800px'}
                ),
                

                # No voicemails table
                html.Div([
                    html.H3(
                        f"Users With No Voicemails",
                        style={
                            'text-align': 'center',
                            'font-size': '20px',
                            'font-weight': 'bold',
                            'font-family': '-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif,"Apple Color Emoji","Segoe UI Emoji","Segoe UI Symbol"'
                        }
                    ),
                    dash_table.DataTable(
                        id='no-vm-table',
                        columns=[{'name': 'User ID', 'id': 'User ID'}],
                        style_table={'width': '300px', 'margin': 'auto'},
                        style_cell={'textAlign': 'center'},
                        style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'}
                    )
                ], style={'marginBottom': '20px'}),
                
                # Nested bar chart moved to bottom
                dcc.Graph(
                    id='vm-nested-chart',
                    style={'height': '800px'}
                ),
                
            ], style={'height': 'auto', 'minHeight': '2000px'})
        ]),
        dcc.Tab(label='Clinic Aggregate Trends', children=[
            html.Div([
                dcc.Graph(id='clinic-vm-trend-graph')
            ])
        ]),
    ])
])

# Split the callbacks
@app.callback(
    Output('volume-heatmap', 'figure'),
    [Input('lookback-dropdown', 'value')]
)
def update_volume_heatmap(selected_days):
    try:
        call_data = fetch_call_data(selected_days)
        if call_data.empty:
            return px.scatter(title="No data available. Please try again.")
        processed_data = process_call_data(call_data)
        return create_call_heatmap(processed_data)
    except Exception as e:
        print(f"Error updating volume heatmap: {e}")
        return px.scatter(title="Error loading data. Please try again later.")

@app.callback(
    Output('pickup-heatmap', 'figure'),
    [Input('lookback-dropdown-2', 'value')]
)
def update_pickup_heatmap(selected_days):
    try:
        call_data = fetch_call_data(selected_days)
        if call_data.empty:
            return px.scatter(title="No data available. Please try again.")
        processed_data = process_call_data(call_data)
        return create_pickup_heatmap(processed_data)
    except Exception as e:
        print(f"Error updating pickup heatmap: {e}")
        return px.scatter(title=f"Error updating dashboard: {str(e)}")

@app.callback(
    [Output('vm-nested-chart', 'figure'),
     Output('vm-response-chart', 'figure'),
     Output('vm-nonresponse-chart', 'figure'),
     Output('no-vm-table', 'data')],
    [Input('vm-window-dropdown', 'value'),
     Input('department-dropdown', 'value')]
)
def update_vm_visualizations(vm_window, selected_department):
    try:
        call_data = fetch_call_data(None)
        if call_data.empty:
            return px.scatter(title="No data available"), px.scatter(title="No data available"), px.scatter(title="No data available"), []
        
        processed_data = process_call_data(call_data)
        
        # Filter by department if not "all"
        if selected_department != 'all':
            # Get user IDs for the selected department
            department_users = [
                user_id for user_id, departments in DEPARTMENT_MAPPING.items()
                if selected_department in departments.split(',')
            ]
            processed_data = processed_data[
                processed_data[COLUMN_MAPPING['user_id']].astype(str).isin(department_users)
            ]
        
        # Get users with no voicemails (existing logic)
        users_with_vm = processed_data[
            (processed_data['direction'] == 'inbound') & 
            (processed_data['voicemail'] == True) &
            processed_data['external_number'].notna()
        ][COLUMN_MAPPING['user_id']].unique()
        
        all_users = processed_data[COLUMN_MAPPING['user_id']].unique()
        users_no_vm = [{'User ID': get_user_name(uid)} for uid in all_users if uid not in users_with_vm]
        
        return (
            create_vm_nested_chart(processed_data, vm_window),
            create_vm_response_chart(processed_data, vm_window),
            create_vm_nonresponse_chart(processed_data, vm_window),
            users_no_vm
        )
    except Exception as e:
        print(f"Error updating visualizations: {e}")
        return px.scatter(title=f"Error updating dashboard: {str(e)}"), px.scatter(title="Error"), px.scatter(title="Error"), []

def create_vm_response_chart(call_data, response_window_hours=24):
    """
    Creates a horizontal bar chart showing voicemail response rates by user
    """
    # Safety check: Ensure datetime
    if not pd.api.types.is_datetime64_any_dtype(call_data[COLUMN_MAPPING['start_time']]):
        call_data[COLUMN_MAPPING['start_time']] = pd.to_datetime(call_data[COLUMN_MAPPING['start_time']])

    # Get date range for display
    start_date = call_data[COLUMN_MAPPING['start_time']].min()
    end_date = call_data[COLUMN_MAPPING['start_time']].max()
    date_range = f"{start_date.strftime('%m/%d/%Y')} - {end_date.strftime('%m/%d/%Y')}"

    vm_stats = []
    for user_id, user_calls in call_data.groupby(COLUMN_MAPPING['user_id']):
        # Get valid voicemails
        voicemails = user_calls[
            (user_calls['direction'] == 'inbound') & 
            (user_calls['voicemail'] == True) &
            user_calls['external_number'].notna()
        ]
        
        total_vm = len(voicemails)
        if total_vm == 0:
            continue
            
        responses = 0
        for _, voicemail in voicemails.iterrows():
            callback = user_calls[
                (user_calls['direction'] == 'outbound') &
                (user_calls['external_number'] == voicemail['external_number']) &
                (user_calls[COLUMN_MAPPING['start_time']] > voicemail[COLUMN_MAPPING['start_time']]) &
                (user_calls[COLUMN_MAPPING['start_time']] <= voicemail[COLUMN_MAPPING['start_time']] + pd.Timedelta(hours=response_window_hours))
            ]
            
            if not callback.empty:
                responses += 1
        
        response_rate = (responses / total_vm * 100)
        
        # Only add to vm_stats if they have responses
        if responses > 0:  # Added this condition
            vm_stats.append({
                'User ID': get_user_name(user_id),  # Convert to friendly name
                'Total Voicemails': total_vm,
                'Responses': responses,
                'Response Rate': response_rate
            })

    # Convert to DataFrame and sort by response rate
    stats_df = pd.DataFrame(vm_stats)
    if stats_df.empty:
        return px.scatter(title="No data available")
        
    stats_df = stats_df.sort_values('Response Rate', ascending=True)  # Ascending for horizontal bars
    
    # Calculate average response rate
    avg_response_rate = stats_df['Response Rate'].mean()

    # Create horizontal bar chart
    fig = go.Figure()

    # Add bars
    fig.add_trace(go.Bar(
        y=stats_df['User ID'],
        x=stats_df['Response Rate'],
        orientation='h',
        marker=dict(
            color=stats_df['Response Rate'],
            colorscale='Blues'
        ),
        text=[f'<b>{rate:.1f}%</b>' for rate in stats_df['Response Rate']],
        textposition='outside',
        textfont=dict(size=14),
        hovertemplate=(
            'User ID: %{customdata[0]}<br>' +
            'Total Voicemails: %{customdata[1]}<br>' +
            'Responses: %{customdata[2]}<br>' +
            'Response Rate: %{customdata[3]:.1f}%' +
            '<extra></extra>'
        ),
        customdata=stats_df[['User ID', 'Total Voicemails', 'Responses', 'Response Rate']].values
    ))

    # Add average line
    fig.add_vline(
        x=avg_response_rate,
        line_dash='dot',
        line_color='grey',
        annotation=dict(
            text=f'Average: {avg_response_rate:.1f}%',
            font=dict(size=16, weight='bold'),
            xanchor='left',
            yanchor='bottom',
            xref='x',
            yref='paper',
            x=avg_response_rate + 1,
            y=0,
            showarrow=False
        )
    )

    # Update layout
    window_text = "24 Hours" if response_window_hours == 24 else \
                 "48 Hours" if response_window_hours == 48 else \
                 "72 Hours" if response_window_hours == 72 else \
                 "1 Week" if response_window_hours == 168 else \
                 "2 Weeks"
    
    # Calculate dynamic height (minimum 400px, 50px per bar)
    bar_height = 50  # Height per bar in pixels
    margin_height = 200  # Space for title, axes, margins
    dynamic_height = max(800, (len(stats_df) * bar_height) + margin_height)

    fig.update_layout(
        title=dict(
            text=f"Voicemail Response Rates by User (Within {window_text})<br>({date_range})",
            font=dict(size=20, weight='bold'),
            x = 0.5
        ),
        xaxis_title="Response Rate (%)",
        yaxis_title="User ID",
        xaxis=dict(tickfont=dict(size=14)),
        yaxis=dict(tickfont=dict(size=14)),
        height=dynamic_height,
        margin=dict(l=100, r=150, t=100, b=100),
        showlegend=False,
        plot_bgcolor='white',
        paper_bgcolor='white',
        bargap=0.3,  # Consistent spacing between bars
    )

    return fig

def create_vm_nonresponse_chart(call_data, response_window_hours=24):
    """
    Creates a horizontal bar chart showing total voicemails for users with 0% response rate
    """
    # Get date range for display
    start_date = call_data[COLUMN_MAPPING['start_time']].min()
    end_date = call_data[COLUMN_MAPPING['start_time']].max()
    date_range = f"{start_date.strftime('%m/%d/%Y')} - {end_date.strftime('%m/%d/%Y')}"

    vm_stats = []
    for user_id, user_calls in call_data.groupby(COLUMN_MAPPING['user_id']):
        # Get valid voicemails
        voicemails = user_calls[
            (user_calls['direction'] == 'inbound') & 
            (user_calls['voicemail'] == True) &
            user_calls['external_number'].notna()
        ]
        
        total_vm = len(voicemails)
        if total_vm == 0:
            continue
            
        responses = 0
        for _, voicemail in voicemails.iterrows():
            callback = user_calls[
                (user_calls['direction'] == 'outbound') &
                (user_calls['external_number'] == voicemail['external_number']) &
                (user_calls[COLUMN_MAPPING['start_time']] > voicemail[COLUMN_MAPPING['start_time']]) &
                (user_calls[COLUMN_MAPPING['start_time']] <= voicemail[COLUMN_MAPPING['start_time']] + pd.Timedelta(hours=response_window_hours))
            ]
            
            if not callback.empty:
                responses += 1
        
        # Only include users with 0 responses
        if responses == 0:
            vm_stats.append({
                'User ID': get_user_name(user_id),  # Convert to friendly name
                'Total Voicemails': total_vm
            })

    # Convert to DataFrame and sort by total voicemails
    stats_df = pd.DataFrame(vm_stats)
    if stats_df.empty:
        return px.scatter(title="No data available")
        
    stats_df = stats_df.sort_values('Total Voicemails', ascending=True)

    # Create horizontal bar chart
    fig = go.Figure()

    # Add bars
    fig.add_trace(go.Bar(
        y=stats_df['User ID'],
        x=stats_df['Total Voicemails'],
        orientation='h',
        marker=dict(
            color=stats_df['Total Voicemails'],
            colorscale='Blues'
        ),
        text=[f'<b>{count}</b>' for count in stats_df['Total Voicemails']],
        textposition='outside',
        textfont=dict(size=14),
        hovertemplate=(
            'User ID: %{customdata[0]}<br>' +
            'Total Voicemails: %{customdata[1]}' +
            '<extra></extra>'
        ),
        customdata=stats_df[['User ID', 'Total Voicemails']].values
    ))

    # Update layout
    window_text = "24 Hours" if response_window_hours == 24 else \
                 "48 Hours" if response_window_hours == 48 else \
                 "72 Hours" if response_window_hours == 72 else \
                 "1 Week" if response_window_hours == 168 else \
                 "2 Weeks"
    
    # Calculate dynamic height (minimum 400px, 50px per bar)
    bar_height = 50  # Height per bar in pixels
    margin_height = 200  # Space for title, axes, margins
    dynamic_height = max(400, (len(stats_df) * bar_height) + margin_height)

    fig.update_layout(
        title=dict(
            text=f"Users With No Responses (Within {window_text})<br>({date_range})",
            font=dict(size=20, weight='bold'),
            x = 0.5,
        ),
        xaxis_title="Total Voicemails",
        yaxis_title="User ID",
        xaxis=dict(tickfont=dict(size=14)),
        yaxis=dict(tickfont=dict(size=14)),
        height=dynamic_height,
        margin=dict(l=100, r=150, t=100, b=100),
        showlegend=False,
        plot_bgcolor='white',
        paper_bgcolor='white',
        bargap=0.3,  # Consistent spacing between bars
    )

    return fig

def create_vm_nested_chart(call_data, response_window_hours=24):
    """Creates a nested bar chart showing total voicemails and response rates"""
    # Get date range for display
    start_date = call_data[COLUMN_MAPPING['start_time']].min()
    end_date = call_data[COLUMN_MAPPING['start_time']].max()
    date_range = f"{start_date.strftime('%m/%d/%Y')} - {end_date.strftime('%m/%d/%Y')}"

    # Calculate stats for each user
    vm_stats = []
    for user_id, user_calls in call_data.groupby(COLUMN_MAPPING['user_id']):
        voicemails = user_calls[
            (user_calls['direction'] == 'inbound') & 
            (user_calls['voicemail'] == True) &
            user_calls['external_number'].notna()
        ]
        
        total_vm = len(voicemails)
        if total_vm == 0:
            continue
            
        responses = 0
        for _, voicemail in voicemails.iterrows():
            callback = user_calls[
                (user_calls['direction'] == 'outbound') &
                (user_calls['external_number'] == voicemail['external_number']) &
                (user_calls[COLUMN_MAPPING['start_time']] > voicemail[COLUMN_MAPPING['start_time']]) &
                (user_calls[COLUMN_MAPPING['start_time']] <= voicemail[COLUMN_MAPPING['start_time']] + pd.Timedelta(hours=response_window_hours))
            ]
            
            if not callback.empty:
                responses += 1
        
        response_rate = (responses / total_vm * 100)
        
        vm_stats.append({
            'User ID': get_user_name(user_id),  # Convert to friendly name
            'Total Voicemails': total_vm,
            'Responses': responses,
            'Response Rate': response_rate
        })

    # Convert to DataFrame and sort
    stats_df = pd.DataFrame(vm_stats)
    if stats_df.empty:
        return px.scatter(title="No data available")
        
    stats_df = stats_df.sort_values('Total Voicemails', ascending=True)

    # Create figure
    fig = go.Figure()

    # Add outer bars (total voicemails)
    fig.add_trace(go.Bar(
        y=stats_df['User ID'],
        x=stats_df['Total Voicemails'],
        orientation='h',
        name='Total Voicemails',
        marker=dict(color='rgb(200, 220, 240)'),
        text=[str(count) if (count - responses) > 50 else '' for count, responses in zip(stats_df['Total Voicemails'], stats_df['Responses'])],
        textposition='auto',
        textfont=dict(size=14, color='black'),
        hovertemplate=(
            'User ID: %{customdata[0]}<br>' +
            'Total Voicemails: %{customdata[1]}<br>' +
            'Responses: %{customdata[2]}<br>' +
            'Response Rate: %{customdata[3]:.1f}%' +
            '<extra></extra>'
        ),
        customdata=stats_df[['User ID', 'Total Voicemails', 'Responses', 'Response Rate']].values
    ))

    # Add inner bars (responses)
    fig.add_trace(go.Bar(
        y=stats_df['User ID'],
        x=stats_df['Total Voicemails'] * stats_df['Response Rate'] / 100,
        orientation='h',
        name='Responses',
        marker=dict(color='rgb(0, 87, 168)'),
        text=[str(count) if count > 50 else '' for count in stats_df['Responses']],
        textposition='auto',
        textfont=dict(size=14, color='white'),
        hovertemplate=(
            'User ID: %{customdata[0]}<br>' +
            'Total Voicemails: %{customdata[1]}<br>' +
            'Responses: %{customdata[2]}<br>' +
            'Response Rate: %{customdata[3]:.1f}%' +
            '<extra></extra>'
        ),
        customdata=stats_df[['User ID', 'Total Voicemails', 'Responses', 'Response Rate']].values
    ))

    # Update layout
    window_text = "24 Hours" if response_window_hours == 24 else \
                 "48 Hours" if response_window_hours == 48 else \
                 "72 Hours" if response_window_hours == 72 else \
                 "1 Week" if response_window_hours == 168 else \
                 "2 Weeks"
    
    # Calculate dynamic height (increase the multiplier and minimum height)
    bar_height = 50  # Height per bar in pixels
    margin_height = 200  # Space for title, axes, margins
    dynamic_height = max(800, (len(stats_df) * bar_height) + margin_height)  # Increased minimum from 400 to 800

    fig.update_layout(
        title=dict(
            text=f"Voicemail Overview (Within {window_text})<br>({date_range})",
            font=dict(size=20, weight='bold'),
            x = 0.5
        ),
        barmode='overlay',
        xaxis_title="Number of Voicemails",
        yaxis_title="User ID",
        xaxis=dict(tickfont=dict(size=14)),
        yaxis=dict(tickfont=dict(size=14)),
        height=dynamic_height,  # This will now be much taller
        margin=dict(l=100, r=150, t=100, b=100),  # Increased bottom margin
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        plot_bgcolor='white',
        paper_bgcolor='white',
        bargap=0.2,
    )

    return fig

def create_trend_graph(call_data):
    """Creates a line graph showing weekly voicemail response rates and pickup rates"""
    try:
        # Get date range for display
        start_date = call_data[COLUMN_MAPPING['start_time']].min()
        end_date = call_data[COLUMN_MAPPING['start_time']].max()
        date_range = f"{start_date.strftime('%m/%d/%Y')} - {end_date.strftime('%m/%d/%Y')}"
        
        # Group by week and calculate stats
        weekly_stats = []
        for week_start, week_data in call_data.groupby(pd.Grouper(key=COLUMN_MAPPING['start_time'], freq='W')):
            # Calculate VM response rate
            voicemails = week_data[
                (week_data['direction'] == 'inbound') & 
                (week_data['voicemail'] == True) &
                week_data['external_number'].notna()
            ]
            
            total_vm = len(voicemails)
            vm_responses = 0
            if total_vm > 0:
                for _, voicemail in voicemails.iterrows():
                    callback = week_data[
                        (week_data['direction'] == 'outbound') &
                        (week_data['external_number'] == voicemail['external_number']) &
                        (week_data[COLUMN_MAPPING['start_time']] > voicemail[COLUMN_MAPPING['start_time']]) &
                        (week_data[COLUMN_MAPPING['start_time']] <= voicemail[COLUMN_MAPPING['start_time']] + pd.Timedelta(hours=24))
                    ]
                    if not callback.empty:
                        vm_responses += 1
            
            # Calculate pickup rate
            inbound_calls = week_data[week_data['direction'] == 'inbound']
            total_calls = len(inbound_calls)
            answered_calls = len(inbound_calls[inbound_calls[COLUMN_MAPPING['connect_time']].notna()])
            
            # Calculate rates
            vm_response_rate = (vm_responses / total_vm * 100) if total_vm > 0 else 0
            pickup_rate = (answered_calls / total_calls * 100) if total_calls > 0 else 0
            
            weekly_stats.append({
                'Week': week_start,
                'Total_VM': total_vm,
                'VM_Responses': vm_responses,
                'VM_Response_Rate': vm_response_rate,
                'Total_Calls': total_calls,
                'Answered_Calls': answered_calls,
                'Pickup_Rate': pickup_rate
            })
        
        # Convert to DataFrame
        stats_df = pd.DataFrame(weekly_stats)
        if stats_df.empty:
            return px.scatter(title="No data available")
        
        # Create figure
        fig = go.Figure()
        
        # Add VM response rate line
        fig.add_trace(go.Scatter(
            x=stats_df['Week'],
            y=stats_df['VM_Response_Rate'],
            name='VM Response Rate',
            mode='lines+markers',
            line=dict(color='rgb(0, 76, 153)', width=2),
            marker=dict(size=8),
            hovertemplate=(
                '<b>Week of %{x|%b %d, %Y}</b><br>' +
                'Total Voicemails: %{customdata[0]}<br>' +
                'Voicemails Responded: %{customdata[1]}<br>' +
                'Response Rate: %{y:.1f}%<extra></extra>'
            ),
            customdata=stats_df[['Total_VM', 'VM_Responses']].values
        ))
        
        # Add pickup rate line
        fig.add_trace(go.Scatter(
            x=stats_df['Week'],
            y=stats_df['Pickup_Rate'],
            name='Pickup Rate',
            mode='lines+markers',
            line=dict(color='rgb(0, 168, 107)', width=2),
            marker=dict(size=8),
            hovertemplate=(
                '<b>Week of %{x|%b %d, %Y}</b><br>' +
                'Total Calls: %{customdata[0]}<br>' +
                'Answered Calls: %{customdata[1]}<br>' +
                'Pickup Rate: %{y:.1f}%<extra></extra>'
            ),
            customdata=stats_df[['Total_Calls', 'Answered_Calls']].values
        ))
        
        fig.update_layout(
            title={
                'text': f'Weekly Call Metrics<br>({date_range})',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 20, 'weight': 'bold'}
            },
            xaxis_title='Week',
            yaxis_title='Rate (%)',
            yaxis_range=[0, 100],
            hovermode='x unified',
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            ),
            template='plotly_white',
            height=400,
            margin=dict(l=50, r=20, t=100, b=50)
        )
        
        return fig
        
    except Exception as e:
        print(f"Error creating trend graph: {e}")
        return px.scatter(title=f"Error creating trend graph: {str(e)}")

@app.callback(
    Output('clinic-vm-trend-graph', 'figure'),
    Input('interval-component', 'n_intervals')  # Use interval component instead of store
)
def update_trend_graph(_):
    return create_trend_graph(call_data)  # Using global call_data

# Run the Dash app
if __name__ == '__main__':
    app.run_server(debug=True)
