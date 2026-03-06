import sqlite3
import os

print("🔄 Starting export of attendance.db...")

conn = sqlite3.connect('attendance.db')
with open('/tmp/dump.sql', 'w', encoding='utf-8') as f:
    for line in conn.iterdump():
        f.write('%s\n' % line)
conn.close()

print('✅ Dump created successfully!')
print('File size:', os.path.getsize('/tmp/dump.sql'), 'bytes')