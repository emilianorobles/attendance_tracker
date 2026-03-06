import sqlite3

print("🔄 Starting database export...")
conn = sqlite3.connect('attendance.db')
for line in conn.iterdump():
    print(line)
conn.close()
print("✅ Dump completed successfully!")