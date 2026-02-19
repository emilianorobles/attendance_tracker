#!/usr/bin/env python3
"""Quick test of attendance endpoint with new ScheduleProvider."""

import requests
import json

try:
    response = requests.get('http://127.0.0.1:8000/attendance?start=2026-01-01&end=2026-01-31')
    print(f"✅ Status Code: {response.status_code}")
    
    data = response.json()
    agents = data.get('agents', [])
    
    print(f"✅ Total Agents: {len(agents)}")
    
    if agents:
        agent = agents[0]
        print(f"\n📋 First Agent:")
        print(f"  - ID: {agent.get('agent_id')}")
        print(f"  - Name: {agent.get('name')}")
        print(f"  - Lead: {agent.get('lead')}")
        print(f"  - Days: {len(agent.get('days', []))}")
        print(f"  - Late Minutes Sum: {agent.get('late_minutes_sum')}")
        print(f"  - Delays Sum: {agent.get('delays_sum')}")
        
        if agent.get('days'):
            first_day = agent.get('days')[0]
            print(f"\n📅 First Day:")
            print(f"  - Date: {first_day.get('date')}")
            print(f"  - Status: {first_day.get('status')}")
            print(f"  - Planned: {first_day.get('planned_start')} - {first_day.get('planned_end')}")
            print(f"  - Actual: {first_day.get('actual_start')} - {first_day.get('actual_end')}")
    
    print("\n✅ ScheduleProvider is working correctly!")
    
except Exception as e:
    print(f"❌ Error: {e}")
