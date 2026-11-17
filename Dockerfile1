# ═════════════════════════════════════════════════════════
#  البوت — استراتيجية Triple Confluence Navigator
#  ملفان فقط: bot.py + web_panel.py
#  المكتبات مثبتة هنا مباشرة — لا حاجة لملف requirements.txt
# ═════════════════════════════════════════════════════════
FROM python:3.11-slim

WORKDIR /bot

# المكتبات الست المطلوبة فقط — كلها بحزم جاهزة (بدون مترجم)
RUN pip install --no-cache-dir \
    "aiohttp>=3.9" \
    "websockets>=11" \
    "aiosqlite>=0.19" \
    "pandas>=2.0" \
    "numpy>=1.24" \
    "requests>=2.31"

# ⬅️ ملفان فقط — مستحيل أن يفلت ملف من القائمة
COPY bot.py web_panel.py ./

RUN mkdir -p /bot/logs
EXPOSE 8080

# ✅ فحص الصحة — Railway يستخدم هذا لمعرفة أن التطبيق يستجيب.
# PORT افتراضياً 8080 على Railway، نقوم بفحص /health كل 30 ثانية.
# نمنح التطبيق 90 ثانية للبدء (start-period) قبل أن نُفشل النشر.
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=5 \
  CMD python -c "import urllib.request,sys; \
r=urllib.request.urlopen('http://127.0.0.1:'+__import__('os').environ.get('PORT','8080')+'/health',timeout=8); \
exit(0 if r.status==200 else 1)" || exit 1

# المحرك — استراتيجية Triple Confluence Navigator (Cardwell + ARSI + Structure)
CMD ["python", "-u", "bot.py"]
