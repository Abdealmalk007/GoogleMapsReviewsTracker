# Google Maps Reviews Tracker v41 — نسخة تطوير

التطبيق مبني باستخدام Python/Streamlit مع SQLite لمتابعة الفروع والمراجعات والتحليلات والتقارير.

## التشغيل في جهاز التطوير
- مسار المشروع: `E:\ProTech\GoogleMapsReviewsTracker\work_v41`.
- تشغيل النسخة المحلية: `RUN_V41_DEV.bat` (المسار الحالي مخصص لهذا الجهاز فقط).
- لا تتضمن حزمة المصدر بيانات العملاء أو `reviews.db` أو `.api_key`، لذلك يلزم إعداد قاعدة اختبار مستقلة.

## مصادر البيانات
- Places API (New): بيانات الأماكن وعينة تصل إلى خمس مراجعات للمكان، ولا تعني جميع مراجعات الفرع.
- Business Profile API: `business_profile_v41.py` مع OAuth صريح لقراءة مراجعات الفروع الموثقة المملوكة؛ تتوفر معاينة قراءة فقط في صفحة الاتصالات، لكن يلزم OAuth مصرح به.
- محرك Browser القديم موجود، لكنه يحتاج اختبارًا حقيقيًا وتقييمًا لسياسات استخدامه.

## الاختبارات
```powershell
python test_v41_offline.py
python test_s2_v41.py
python test_s2_sync.py
python test_gbp_v41.py
```
كل اختبارات الحزمة تستخدم بيانات اختبار صناعية.

## الحالة
v41.0.0-dev — نسخة تطوير، لا تزال تحتاج اعتماد الاختبارات الحية قبل النشر العام.
المتبقي S2-06 للتحقق الحي، ثم S3 للبناء وGitHub والنشر على alghad.digital.

## المراجع الرسمية
- https://developers.google.com/maps/documentation/places/web-service/text-search
- https://developers.google.com/my-business/reference/rest/v4/accounts.locations.reviews/list
- https://cloud.google.com/maps-platform/terms
