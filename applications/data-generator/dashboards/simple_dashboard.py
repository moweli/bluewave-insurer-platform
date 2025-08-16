#!/usr/bin/env python3
"""Simplified monitoring dashboard for insurance data streaming."""

import json
from pathlib import Path
from datetime import datetime
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))


def read_streaming_data(file_path: Path, limit: int = 10):
    """Read the latest events from a streaming file."""
    if not file_path.exists():
        return []
    
    events = []
    with open(file_path, 'r') as f:
        lines = f.readlines()
        for line in lines[-limit:]:  # Get last N events
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


def display_dashboard():
    """Display a simple text-based dashboard."""
    output_dir = Path("streaming_output")
    
    print("\n" + "=" * 80)
    print(" " * 20 + "BLUEWAVE INSURANCE MONITORING DASHBOARD")
    print("=" * 80)
    print(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 80)
    
    # Read claims
    claims = read_streaming_data(output_dir / "claims_stream.jsonl", 20)
    policies = read_streaming_data(output_dir / "policies_stream.jsonl", 10)
    audit = read_streaming_data(output_dir / "audit_stream.jsonl", 10)
    
    # Statistics
    total_claims = len(claims)
    fraud_claims = sum(1 for c in claims if c.get('data', {}).get('claim', {}).get('fraud_score', 0) > 0.7)
    high_value_claims = sum(1 for c in claims if c.get('data', {}).get('claim', {}).get('claim_amount', 0) > 100000)
    
    print("\n📊 KEY METRICS")
    print("-" * 40)
    print(f"  Total Claims Processed: {total_claims}")
    print(f"  High Risk Claims: {fraud_claims}")
    print(f"  High Value Claims (>£100k): {high_value_claims}")
    print(f"  Active Policies: {len(policies)}")
    print(f"  Audit Events: {len(audit)}")
    
    # Fraud Alerts
    print("\n🚨 FRAUD ALERTS")
    print("-" * 40)
    fraud_found = False
    for event in claims:
        claim = event.get('data', {}).get('claim', {})
        fraud_score = claim.get('fraud_score', 0)
        if fraud_score > 0.7:
            fraud_found = True
            print(f"  ⚠️  Claim #{claim.get('claim_number', 'N/A')[-5:]}")
            print(f"     Score: {fraud_score:.2%}")
            print(f"     Amount: £{claim.get('claim_amount', 0):,.2f}")
            print(f"     Type: {claim.get('claim_type', 'N/A')}")
            print()
    
    if not fraud_found:
        print("  ✅ No high-risk claims detected")
    
    # Recent Claims
    print("\n📋 RECENT CLAIMS")
    print("-" * 40)
    print(f"{'Claim ID':<15} {'Type':<15} {'Amount':<15} {'Fraud Score':<12} {'Status'}")
    print("-" * 70)
    
    for event in claims[-10:]:
        claim = event.get('data', {}).get('claim', {})
        claim_id = claim.get('claim_number', 'N/A')[-5:]
        claim_type = claim.get('claim_type', 'N/A')[:12]
        amount = f"£{claim.get('claim_amount', 0):,.0f}"
        fraud_score = claim.get('fraud_score', 0)
        status = claim.get('status', 'N/A')
        
        # Color coding for terminal (simplified)
        fraud_indicator = "🔴" if fraud_score > 0.7 else "🟡" if fraud_score > 0.4 else "🟢"
        
        print(f"{claim_id:<15} {claim_type:<15} {amount:<15} {fraud_score:.2%} {fraud_indicator:<5} {status}")
    
    # Geographic Distribution
    print("\n📍 GEOGRAPHIC DISTRIBUTION")
    print("-" * 40)
    cities = {}
    for event in claims:
        city = event.get('data', {}).get('claim', {}).get('location', {}).get('city', 'Unknown')
        cities[city] = cities.get(city, 0) + 1
    
    sorted_cities = sorted(cities.items(), key=lambda x: x[1], reverse=True)[:5]
    for city, count in sorted_cities:
        bar = "█" * min(count * 2, 40)
        print(f"  {city:<20} {bar} ({count})")
    
    # Summary
    print("\n" + "=" * 80)
    print(f"Dashboard refreshed at {datetime.now().strftime('%H:%M:%S')}")
    print("Press Ctrl+C to exit")
    print("=" * 80)


def main():
    """Main dashboard loop."""
    import time
    
    try:
        while True:
            # Clear screen (works on Unix-like systems)
            print("\033[2J\033[H", end="")
            
            display_dashboard()
            
            # Refresh every 5 seconds
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\n✅ Dashboard closed")
    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    # Check if streaming data exists
    output_dir = Path("streaming_output")
    if not output_dir.exists():
        print("❌ No streaming data found. Please run the streaming demo first:")
        print("   python src/local_streaming_demo.py")
        sys.exit(1)
    
    main()