railway=# SET client_encoding = 'UTF8';
SET
railway=#
railway=# SELECT 'meat_grill_blood_sausages/ribs' AS test_bucket, pg.id, pg.canonical_name, pg.brand, pg.sub_code,
railway-#        array_agg(DISTINCT s.chain ORDER BY s.chain) AS chains_with_price
railway-# FROM product_groups pg
railway-# JOIN product_group_members m ON m.group_id = pg.id
railway-# JOIN products p ON p.id = m.product_id
railway-# JOIN prices pr ON pr.product_id = p.id AND pr.price IS NOT NULL AND pr.price > 0
railway-# JOIN stores s ON s.id = pr.store_id
railway-# WHERE pg.sub_code = 'meat_grill_blood_sausages'
railway-#   AND (p.name ILIKE '%ribi%' OR p.name ILIKE '%steik%' OR p.name ILIKE '%toorvorst%'
railway(#        OR p.name ILIKE '%praevorst%' OR p.name ILIKE '%verik%')
railway-# GROUP BY pg.id, pg.canonical_name, pg.brand, pg.sub_code
railway-# HAVING COUNT(DISTINCT s.chain) < 5
railway-#
railway-# UNION ALL
railway-#
railway-# SELECT 'meat_poultry/turkey_duck', pg.id, pg.canonical_name, pg.brand, pg.sub_code,
railway-#        array_agg(DISTINCT s.chain ORDER BY s.chain)
railway-# FROM product_groups pg
railway-# JOIN product_group_members m ON m.group_id = pg.id
railway-# JOIN products p ON p.id = m.product_id
railway-# JOIN prices pr ON pr.product_id = p.id AND pr.price IS NOT NULL AND pr.price > 0
railway-# JOIN stores s ON s.id = pr.store_id
railway-# WHERE pg.sub_code = 'meat_poultry'
railway-#   AND (p.name ILIKE '%kalkun%' OR p.name ILIKE '%part%' OR p.name ILIKE '%pardi%')
railway-# GROUP BY pg.id, pg.canonical_name, pg.brand, pg.sub_code
railway-# HAVING COUNT(DISTINCT s.chain) < 5
railway-#
railway-# UNION ALL
railway-#
railway-# SELECT 'meat_sausages/cross_species', pg.id, pg.canonical_name, pg.brand, pg.sub_code,
railway-#        array_agg(DISTINCT s.chain ORDER BY s.chain)
railway-# FROM product_groups pg
railway-# JOIN product_group_members m ON m.group_id = pg.id
railway-# JOIN products p ON p.id = m.product_id
railway-# JOIN prices pr ON pr.product_id = p.id AND pr.price IS NOT NULL AND pr.price > 0
railway-# JOIN stores s ON s.id = pr.store_id
railway-# WHERE pg.sub_code = 'meat_sausages'
railway-# GROUP BY pg.id, pg.canonical_name, pg.brand, pg.sub_code
railway-# HAVING COUNT(DISTINCT s.chain) < 5
railway-# LIMIT 15
railway-#
railway-# UNION ALL
railway-#
railway-# SELECT 'coffee_beans_ground/brand_diversity', pg.id, pg.canonical_name, pg.brand, pg.sub_code,
railway-#        array_agg(DISTINCT s.chain ORDER BY s.chain)
railway-# FROM product_groups pg
railway-# JOIN product_group_members m ON m.group_id = pg.id
railway-# JOIN products p ON p.id = m.product_id
railway-# JOIN prices pr ON pr.product_id = p.id AND pr.price IS NOT NULL AND pr.price > 0
railway-# JOIN stores s ON s.id = pr.store_id
railway-# WHERE pg.sub_code = 'coffee_beans_ground'
railway-#   AND pg.brand NOT IN ('Jacobs', 'Paulig', 'Lavazza')
railway-# GROUP BY pg.id, pg.canonical_name, pg.brand, pg.sub_code
railway-# HAVING COUNT(DISTINCT s.chain) < 5
railway-# LIMIT 10
railway-#
railway-# UNION ALL
railway-#
railway-# SELECT 'oils_olive/brand_diversity', pg.id, pg.canonical_name, pg.brand, pg.sub_code,
railway-#        array_agg(DISTINCT s.chain ORDER BY s.chain)
railway-# FROM product_groups pg
railway-# JOIN product_group_members m ON m.group_id = pg.id
railway-# JOIN products p ON p.id = m.product_id
railway-# JOIN prices pr ON pr.product_id = p.id AND pr.price IS NOT NULL AND pr.price > 0
railway-# JOIN stores s ON s.id = pr.store_id
railway-# WHERE pg.sub_code = 'oils_olive'
railway-#   AND pg.brand NOT IN ('Borges')
railway-# GROUP BY pg.id, pg.canonical_name, pg.brand, pg.sub_code
railway-# HAVING COUNT(DISTINCT s.chain) < 5
railway-# LIMIT 10
railway-#
railway-# UNION ALL
railway-#
railway-# SELECT 'never_tested_subcodes', pg.id, pg.canonical_name, pg.brand, pg.sub_code,
railway-#        array_agg(DISTINCT s.chain ORDER BY s.chain)
railway-# FROM product_groups pg
railway-# JOIN product_group_members m ON m.group_id = pg.id
railway-# JOIN products p ON p.id = m.product_id
railway-# JOIN prices pr ON pr.product_id = p.id AND pr.price IS NOT NULL AND pr.price > 0
railway-# JOIN stores s ON s.id = pr.store_id
railway-# WHERE pg.sub_code IN ('drinks_soft_soda', 'spices_herbs_spice_mix', 'baby_porridge_cereal',
railway(#                        'pet_cat_wet', 'hh_paper', 'wine_red', 'fish_fresh')
railway-# GROUP BY pg.id, pg.canonical_name, pg.brand, pg.sub_code
railway-# HAVING COUNT(DISTINCT s.chain) < 5
railway-# ORDER BY 1
railway-# LIMIT 30;
ERROR:  syntax error at or near "UNION"
LINE 37: UNION ALL
         ^
railway=# SET client_encoding = 'UTF8';
SET
railway=#
railway=# (SELECT 'meat_grill_blood_sausages_ribs' AS test_bucket, pg.id, pg.canonical_name, pg.brand, pg.sub_code,
railway(#        array_agg(DISTINCT s.chain ORDER BY s.chain) AS chains_with_price
railway(# FROM product_groups pg
railway(# JOIN product_group_members m ON m.group_id = pg.id
railway(# JOIN products p ON p.id = m.product_id
railway(# JOIN prices pr ON pr.product_id = p.id AND pr.price IS NOT NULL AND pr.price > 0
railway(# JOIN stores s ON s.id = pr.store_id
railway(# WHERE pg.sub_code = 'meat_grill_blood_sausages'
railway(#   AND (p.name ILIKE '%ribi%' OR p.name ILIKE '%steik%' OR p.name ILIKE '%toorvorst%'
railway(#        OR p.name ILIKE '%praevorst%' OR p.name ILIKE '%verik%')
railway(# GROUP BY pg.id, pg.canonical_name, pg.brand, pg.sub_code
railway(# HAVING COUNT(DISTINCT s.chain) < 5)
railway-#
railway-# UNION ALL
railway-#
railway-# (SELECT 'meat_poultry_turkey_duck', pg.id, pg.canonical_name, pg.brand, pg.sub_code,
railway(#        array_agg(DISTINCT s.chain ORDER BY s.chain)
railway(# FROM product_groups pg
railway(# JOIN product_group_members m ON m.group_id = pg.id
railway(# JOIN products p ON p.id = m.product_id
railway(# JOIN prices pr ON pr.product_id = p.id AND pr.price IS NOT NULL AND pr.price > 0
railway(# JOIN stores s ON s.id = pr.store_id
railway(# WHERE pg.sub_code = 'meat_poultry'
railway(#   AND (p.name ILIKE '%kalkun%' OR p.name ILIKE '%part%' OR p.name ILIKE '%pardi%')
railway(# GROUP BY pg.id, pg.canonical_name, pg.brand, pg.sub_code
railway(# HAVING COUNT(DISTINCT s.chain) < 5)
railway-#
railway-# UNION ALL
railway-#
railway-# (SELECT 'meat_sausages_cross_species', pg.id, pg.canonical_name, pg.brand, pg.sub_code,
railway(#        array_agg(DISTINCT s.chain ORDER BY s.chain)
railway(# FROM product_groups pg
railway(# JOIN product_group_members m ON m.group_id = pg.id
railway(# JOIN products p ON p.id = m.product_id
railway(# JOIN prices pr ON pr.product_id = p.id AND pr.price IS NOT NULL AND pr.price > 0
railway(# JOIN stores s ON s.id = pr.store_id
railway(# WHERE pg.sub_code = 'meat_sausages'
railway(# GROUP BY pg.id, pg.canonical_name, pg.brand, pg.sub_code
railway(# HAVING COUNT(DISTINCT s.chain) < 5
railway(# LIMIT 15)
railway-#
railway-# UNION ALL
railway-#
railway-# (SELECT 'coffee_beans_ground_brand_diversity', pg.id, pg.canonical_name, pg.brand, pg.sub_code,
railway(#        array_agg(DISTINCT s.chain ORDER BY s.chain)
railway(# FROM product_groups pg
railway(# JOIN product_group_members m ON m.group_id = pg.id
railway(# JOIN products p ON p.id = m.product_id
railway(# JOIN prices pr ON pr.product_id = p.id AND pr.price IS NOT NULL AND pr.price > 0
railway(# JOIN stores s ON s.id = pr.store_id
railway(# WHERE pg.sub_code = 'coffee_beans_ground'
railway(#   AND pg.brand NOT IN ('Jacobs', 'Paulig', 'Lavazza')
railway(# GROUP BY pg.id, pg.canonical_name, pg.brand, pg.sub_code
railway(# HAVING COUNT(DISTINCT s.chain) < 5
railway(# LIMIT 10)
railway-#
railway-# UNION ALL
railway-#
railway-# (SELECT 'oils_olive_brand_diversity', pg.id, pg.canonical_name, pg.brand, pg.sub_code,
railway(#        array_agg(DISTINCT s.chain ORDER BY s.chain)
railway(# FROM product_groups pg
railway(# JOIN product_group_members m ON m.group_id = pg.id
railway(# JOIN products p ON p.id = m.product_id
railway(# JOIN prices pr ON pr.product_id = p.id AND pr.price IS NOT NULL AND pr.price > 0
railway(# JOIN stores s ON s.id = pr.store_id
railway(# WHERE pg.sub_code = 'oils_olive'
railway(#   AND pg.brand NOT IN ('Borges')
railway(# GROUP BY pg.id, pg.canonical_name, pg.brand, pg.sub_code
railway(# HAVING COUNT(DISTINCT s.chain) < 5
railway(# LIMIT 10)
railway-#
railway-# UNION ALL
railway-#
railway-# (SELECT 'never_tested_subcodes', pg.id, pg.canonical_name, pg.brand, pg.sub_code,
railway(#        array_agg(DISTINCT s.chain ORDER BY s.chain)
railway(# FROM product_groups pg
railway(# JOIN product_group_members m ON m.group_id = pg.id
railway(# JOIN products p ON p.id = m.product_id
railway(# JOIN prices pr ON pr.product_id = p.id AND pr.price IS NOT NULL AND pr.price > 0
railway(# JOIN stores s ON s.id = pr.store_id
railway(# WHERE pg.sub_code IN ('drinks_soft_soda', 'spices_herbs_spice_mix', 'baby_porridge_cereal',
railway(#                        'pet_cat_wet', 'hh_paper', 'wine_red', 'fish_fresh')
railway(# GROUP BY pg.id, pg.canonical_name, pg.brand, pg.sub_code
railway(# HAVING COUNT(DISTINCT s.chain) < 5
railway(# ORDER BY 2
railway(# LIMIT 30)
railway-#
railway-# ORDER BY 1;
             test_bucket             |  id   |                         canonical_name                         |     brand      |         sub_code          |      chains_with_price
-------------------------------------+-------+----------------------------------------------------------------+----------------+---------------------------+-----------------------------
 coffee_beans_ground_brand_diversity | 25616 | Starbucks Blonde Espresso kohvioad 450g                        | Starbucks      | coffee_beans_ground       | {Maxima,Rimi,Selver}
 coffee_beans_ground_brand_diversity | 25615 | Starbucks Pike Place r├Âst 450g                                 | Starbucks      | coffee_beans_ground       | {Prisma,Rimi}
 coffee_beans_ground_brand_diversity | 25643 | Merrild Crema Dolce kohvioad 1kg                               | Merrild        | coffee_beans_ground       | {Rimi,Selver}
 coffee_beans_ground_brand_diversity | 25640 | Merrild In-Cup 500g jahvatatud kohv                            | Merrild        | coffee_beans_ground       | {Maxima,Rimi}
 coffee_beans_ground_brand_diversity | 25639 | Merrild In-Cup tassikohv 400g                                  | Merrild        | coffee_beans_ground       | {Coop,Prisma,Rimi,Selver}
 coffee_beans_ground_brand_diversity | 25638 | Merrild 103 Mellemristet filtrikohv 500g                       | Merrild        | coffee_beans_ground       | {Coop,Prisma,Selver}
 coffee_beans_ground_brand_diversity | 25637 | LOR Classic kohvioad 1kg                                       | LOR            | coffee_beans_ground       | {Selver}
 coffee_beans_ground_brand_diversity | 25636 | LOR Forza kohvioad 1kg                                         | LOR            | coffee_beans_ground       | {Coop,Maxima}
 coffee_beans_ground_brand_diversity | 25635 | LOR Classique kohvioad 1kg                                     | LOR            | coffee_beans_ground       | {Coop,Maxima,Selver}
 coffee_beans_ground_brand_diversity | 25617 | Starbucks Pike Place kohvioad 450g                             | Starbucks      | coffee_beans_ground       | {Maxima,Rimi}
 meat_grill_blood_sausages_ribs      | 16239 | Karni j├Áhvika toorvorstid 400g                                 | Karni          | meat_grill_blood_sausages | {Coop}
 meat_grill_blood_sausages_ribs      | 16242 | Karni saslokitoorvorstid 400g                                  | Karni          | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16245 | Karni Chill-Dill toorvorstid 400g                              | Karni          | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16246 | Karni ├Áuna toorvorstid 400g                                    | Karni          | meat_grill_blood_sausages | {Rimi,Selver}
 meat_grill_blood_sausages_ribs      | 16256 | Matsimoka toorvorstid paprika ja tsilliga lambasooles 300g     | Matsimoka      | meat_grill_blood_sausages | {Coop,Prisma,Selver}
 meat_grill_blood_sausages_ribs      | 16257 | Matsimoka toorvorstid sibula ja ├ñ├ñdikal 300g                   | Matsimoka      | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16264 | Noo sasloki toorvorstid 400g                                   | N├ÁO            | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16267 | Noo laste toorvorstid juustuga 400g                            | N├ÁO            | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16268 | Noo toorvorstid sealihast Nomps 400g                           | N├ÁO            | meat_grill_blood_sausages | {Maxima,Rimi}
 meat_grill_blood_sausages_ribs      | 16274 | Linnam├ñe juustused toorvorstid 900g                            | Linnam├ñe       | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16275 | Linnam├ñe toorvorstid musta k├╝├╝slauguga 350g                    | Linnam├ñe       | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16276 | Linnam├ñe klassikalised sibula-├ñ├ñdika toorvorstid 900g          | Linnam├ñe       | meat_grill_blood_sausages | {Coop,Selver}
 meat_grill_blood_sausages_ribs      | 16291 | Toored broileri toorvorstid urtidega 400g                      | Well Done      | meat_grill_blood_sausages | {Maxima}
 meat_grill_blood_sausages_ribs      | 16292 | Peened toorvorstid sealihast 400g                              | Well Done      | meat_grill_blood_sausages | {Maxima}
 meat_grill_blood_sausages_ribs      | 16294 | Rannam├Áisa toorvorstid broilerilihast tomati-sulajuustuga 300g | Rannam├Áisa     | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16295 | Rannam├Áisa toorvorstid kalkuni kintsulihast 300g               | Rannam├Áisa     | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16302 | Oskar suviste v├╝rtsidega toorvorst 450g                        | Oskar          | meat_grill_blood_sausages | {Prisma,Selver}
 meat_grill_blood_sausages_ribs      | 16167 | Selver Grillsteik sea kaelakarbonaadist kg                     |                | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16311 | Linnam├ñe BBQ grillribi 600g                                    | Linnam├ñe       | meat_grill_blood_sausages | {Rimi}
 meat_grill_blood_sausages_ribs      | 16312 | Rakvere Grill-Ribi kg                                          | Rakvere        | meat_grill_blood_sausages | {Coop,Prisma,Selver}
 meat_grill_blood_sausages_ribs      | 16313 | Rakvere American BBQ grill-ribi 900g                           | Rakvere        | meat_grill_blood_sausages | {Prisma}
 meat_grill_blood_sausages_ribs      | 16314 | Rakvere Mustika Grill-Ribi 1kg                                 | Rakvere        | meat_grill_blood_sausages | {Maxima,Prisma,Rimi,Selver}
 meat_grill_blood_sausages_ribs      | 16315 | Rakvere mustikamarinaadis ribi 900g                            | Rakvere        | meat_grill_blood_sausages | {Rimi}
 meat_grill_blood_sausages_ribs      | 16316 | Rakvere aasiaparne kauakupsenud grillribi 900g                 | Rakvere        | meat_grill_blood_sausages | {Prisma,Rimi}
 meat_grill_blood_sausages_ribs      | 16317 | Rannarootsi Teriyaki grillribid 900g eelkupsetatud             | Rannarootsi    | meat_grill_blood_sausages | {Coop,Prisma,Rimi,Selver}
 meat_grill_blood_sausages_ribs      | 16318 | Rannarootsi kuldne grillribi 900g eelkupsetatud                | Rannarootsi    | meat_grill_blood_sausages | {Prisma,Rimi,Selver}
 meat_grill_blood_sausages_ribs      | 16319 | Smokey BBQ grill-ribi Maks&Moorits kg                          | Maks & Moorits | meat_grill_blood_sausages | {Rimi}
 meat_grill_blood_sausages_ribs      | 16322 | Rakvere verikakk viilutatud 300g                               | Rakvere        | meat_grill_blood_sausages | {Rimi}
 meat_grill_blood_sausages_ribs      | 16323 | Klassikaline verikakk viilutatud M&M 340g                      | Maks & Moorits | meat_grill_blood_sausages | {Coop,Rimi,Selver}
 meat_grill_blood_sausages_ribs      | 16325 | Miniverikakk Rannarootsi 250g                                  | Rannarootsi    | meat_grill_blood_sausages | {Rimi}
 meat_grill_blood_sausages_ribs      | 16343 | Toorvorstikesed LEMMIK kg                                      | Lemmik         | meat_grill_blood_sausages | {Maxima}
 meat_grill_blood_sausages_ribs      | 44333 | Grillvorst MM saksa praevorst 400g                             | Maks & Moorits | meat_grill_blood_sausages | {Coop,Selver}
 meat_grill_blood_sausages_ribs      | 44335 | Grillvorst MM shashlakivorstid 400g                            | Maks & Moorits | meat_grill_blood_sausages | {Coop,Prisma}
 meat_grill_blood_sausages_ribs      | 44342 | Grillvorst NoO kimchi toorvorstid 400g                         | N├ÁO            | meat_grill_blood_sausages | {Rimi}
 meat_grill_blood_sausages_ribs      | 44354 | Grillvorst RR klassikalised 400g seasooles                     | Rannarootsi    | meat_grill_blood_sausages | {Coop,Prisma,Rimi,Selver}
 meat_grill_blood_sausages_ribs      | 44358 | Grillvorst Kotimaista toorvorst 400g                           | Kotimaista     | meat_grill_blood_sausages | {Prisma}
 meat_grill_blood_sausages_ribs      | 44360 | Grillvorst Tallegg merevaigu toorvorst 400g                    | Tallegg        | meat_grill_blood_sausages | {Coop,Maxima,Prisma,Selver}
 meat_grill_blood_sausages_ribs      | 44365 | Grillvorst Linnam├ñe juustused toorvorstid 900g                 | Linnam├ñe       | meat_grill_blood_sausages | {Coop}
 meat_grill_blood_sausages_ribs      | 44372 | Grillvorst Rakvere Pere toorvorstid 400g                       | Rakvere        | meat_grill_blood_sausages | {Coop,Prisma}
 meat_grill_blood_sausages_ribs      | 44373 | Grillvorst Rimi broileri toorvorstid 400g                      | Rimi           | meat_grill_blood_sausages | {Rimi}
 meat_grill_blood_sausages_ribs      | 44374 | Grillvorst Rimi sealihast toorvorstid 400g                     | Rimi           | meat_grill_blood_sausages | {Rimi}
 meat_grill_blood_sausages_ribs      | 44377 | Grillvorst RM laste toorvorstid broileri 300g                  | Rannam├Áisa     | meat_grill_blood_sausages | {Coop}
 meat_grill_blood_sausages_ribs      | 44378 | Grillvorst Karni Cheddari toorvorstid 400g                     | Karni          | meat_grill_blood_sausages | {Coop,Prisma}
 meat_grill_blood_sausages_ribs      | 44379 | Grillvorst Karni tailihast toorvorstid 400g                    | Karni          | meat_grill_blood_sausages | {Coop}
 meat_grill_blood_sausages_ribs      | 44380 | Grillvorst Karni shashlaki toorvorstid 400g                    | Karni          | meat_grill_blood_sausages | {Coop,Prisma}
 meat_grill_blood_sausages_ribs      | 44381 | Grillvorst Karni teravad toorvorstid 400g                      | Karni          | meat_grill_blood_sausages | {Prisma}
 meat_grill_blood_sausages_ribs      | 44386 | Grillvorst RLK merevaigu toorvorstid 400g                      | Rakvere        | meat_grill_blood_sausages | {Coop,Prisma}
 meat_grill_blood_sausages_ribs      | 44632 | Grillvorstid Treski Kimmoos NoO 365g                           | N├ÁO            | meat_grill_blood_sausages | {Maxima}
 meat_grill_blood_sausages_ribs      | 50573 | Sasloki toorvorstid MjaM 400g                                  | Maks & Moorits | meat_grill_blood_sausages | {Maxima}
 meat_grill_blood_sausages_ribs      | 50634 | Noo Fitlap toorvorstid broileriliha 400g                       | N├ÁO            | meat_grill_blood_sausages | {Coop,Maxima,Selver}
 meat_grill_blood_sausages_ribs      | 50641 | Grillsteik suvises marinaadis Matsimoka 500g                   | Matsimoka      | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 50647 | Linnam├ñe R├Âstleiva mekiga grillribid 600g                      | Linnam├ñe       | meat_grill_blood_sausages | {Prisma}
 meat_grill_blood_sausages_ribs      | 50649 | Noo Fitlap grillsteik suitsuse kirsiga 460g                    | N├ÁO            | meat_grill_blood_sausages | {Coop,Prisma,Selver}
 meat_grill_blood_sausages_ribs      | 50651 | Noo Meistrite grillribid 1.2kg                                 | N├ÁO            | meat_grill_blood_sausages | {Prisma}
 meat_grill_blood_sausages_ribs      | 50657 | Rakvere Piprane babyback searibi 700g                          | Rakvere        | meat_grill_blood_sausages | {Prisma}
 meat_grill_blood_sausages_ribs      | 51661 | Grillitud searibi SELVER kg                                    | Selver         | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 51669 | Teravad toorvorstid TULD! KARNI 400g                           | Karni          | meat_grill_blood_sausages | {Maxima}
 meat_grill_blood_sausages_ribs      | 51670 | Toorvorstid Cheddar juustuga KARNI 400g                        | Karni          | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 51671 | Toorvorstid kukeseentega OSKAR 450g                            | Oskar          | meat_grill_blood_sausages | {Maxima}
 meat_grill_blood_sausages_ribs      | 55609 | Eelkups.jaagriribid metssealih. Rannarootsi 600g               | Rannarootsi    | meat_grill_blood_sausages | {Maxima}
 meat_grill_blood_sausages_ribs      | 56707 | Maks ja Moorits grillsnakk ribiribad 1 3kg                     | Maks & Moorits | meat_grill_blood_sausages | {Coop}
 meat_grill_blood_sausages_ribs      | 56712 | ST.Louis ribi tsillines BBQ kg                                 | Selver         | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 56715 | Verikakk 440g                                                  | Rakvere        | meat_grill_blood_sausages | {Coop}
 meat_grill_blood_sausages_ribs      | 58894 | Armeenia searibid 600g                                         | N├òO            | meat_grill_blood_sausages | {Coop}
 meat_grill_blood_sausages_ribs      | 58941 | Suve├Áhtu toorvorst 450g                                        | Oskar          | meat_grill_blood_sausages | {Coop}
 meat_grill_blood_sausages_ribs      | 58943 | Searibi kuivmarinaadis kg                                      | Armeenia Grill | meat_grill_blood_sausages | {Maxima}
 meat_grill_blood_sausages_ribs      | 16169 | Selver Suitsune sartsakas grillribi kg                         |                | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16191 | Rakvere mustika toorvorstid 400g                               | Rakvere        | meat_grill_blood_sausages | {Maxima,Prisma,Rimi,Selver}
 meat_grill_blood_sausages_ribs      | 16192 | Rakvere laste toorvorstid ploomidega 400g                      | Rakvere        | meat_grill_blood_sausages | {Coop,Maxima,Prisma}
 meat_grill_blood_sausages_ribs      | 16194 | Rannarootsi EHE klassikalised toorvorstid 400g                 | Rannarootsi    | meat_grill_blood_sausages | {Coop,Rimi,Selver}
 meat_grill_blood_sausages_ribs      | 16195 | Rannarootsi sulajuustuga toorvorstid 400g                      | Rannarootsi    | meat_grill_blood_sausages | {Coop,Maxima,Prisma,Selver}
 meat_grill_blood_sausages_ribs      | 16196 | Rannarootsi Gruusia toorvorstid 400g                           | Rannarootsi    | meat_grill_blood_sausages | {Coop,Maxima,Prisma,Selver}
 meat_grill_blood_sausages_ribs      | 16197 | Rannarootsi Tzatziki toorvorstid 400g                          | Rannarootsi    | meat_grill_blood_sausages | {Coop,Prisma,Selver}
 meat_grill_blood_sausages_ribs      | 16198 | Rannarootsi laste toorvorstikesed 400g                         | Rannarootsi    | meat_grill_blood_sausages | {Coop,Maxima,Selver}
 meat_grill_blood_sausages_ribs      | 16204 | Rannarootsi rohelise sibulaga toorvorstid 400g                 | Rannarootsi    | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16206 | Rannarootsi miniribi kg                                        | Rannarootsi    | meat_grill_blood_sausages | {Prisma}
 meat_grill_blood_sausages_ribs      | 16205 | Rannarootsi triibuliha toorvorstid 400g                        | Rannarootsi    | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16214 | M&M Maitselt mahedad toorvorstikesed 450g                      | Maks & Moorits | meat_grill_blood_sausages | {Coop}
 meat_grill_blood_sausages_ribs      | 16218 | M&M Fetajuustu-spinati toorvorstikesed 400g                    | Maks & Moorits | meat_grill_blood_sausages | {Rimi,Selver}
 meat_grill_blood_sausages_ribs      | 16219 | M&M Forte juustu toorvorstikesed 400g                          | Maks & Moorits | meat_grill_blood_sausages | {Maxima,Selver}
 meat_grill_blood_sausages_ribs      | 16220 | M&M Kreekap br lihast toorvorstikesed 400g                     | Maks & Moorits | meat_grill_blood_sausages | {Coop,Rimi}
 meat_grill_blood_sausages_ribs      | 16221 | M&M Puhajarve toorvorstikesed 400g                             | Maks & Moorits | meat_grill_blood_sausages | {Maxima,Selver}
 meat_grill_blood_sausages_ribs      | 16224 | M&M Toorvorst paikesekuivatatud tomati ja juustuga 400g        | Maks & Moorits | meat_grill_blood_sausages | {Selver}
 meat_grill_blood_sausages_ribs      | 16227 | M&M Grillribi punases marinaadis ~1.3kg                        | Maks & Moorits | meat_grill_blood_sausages | {Prisma,Selver}
 meat_grill_blood_sausages_ribs      | 16230 | M&M Kolme sibula toorvorstikesed 400g                          | Maks & Moorits | meat_grill_blood_sausages | {Coop,Prisma,Selver}
 meat_grill_blood_sausages_ribs      | 16238 | Karni Jaagri toorvorstid lepasuitsuga 400g                     | Karni          | meat_grill_blood_sausages | {Coop,Prisma}
 meat_poultry_turkey_duck            | 23449 | Hrk peakoka pardirinnafilee Linnam├ñe kg                        | Linnam├Áe       | meat_poultry              | {Selver}
 meat_poultry_turkey_duck            | 23298 | Rannam├Áisa Broiler jahutatud kg                                | Rannam├Áisa     | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 23353 | Kalkuni-suvikarvitsakotlet Selveri kook 240g                   | Selver K├Â├Âk    | meat_poultry              | {Selver}
 meat_poultry_turkey_duck            | 23396 | Kalkuni ┼ía┼íl├Ákk Armeenia Grill kg URL                          | Armeenia Grill | meat_poultry              | {Maxima,Prisma,Rimi}
 meat_poultry_turkey_duck            | 23408 | Kalkunifilee Rannam├Áisa kg                                     | Rannam├Áisa     | meat_poultry              | {Selver}
 meat_poultry_turkey_duck            | 23409 | Kalkunifilee viilud Coop 300g                                  | Coop           | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 23410 | Kalkunirull Coop 700g                                          | Coop           | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 23411 | Kalkuniguljass Rimi 450g                                       | Rimi           | meat_poultry              | {Rimi}
 meat_poultry_turkey_duck            | 23412 | Kalkuniguljass jahutatud WELL DONE 400g                        | Well Done      | meat_poultry              | {Maxima}
 meat_poultry_turkey_duck            | 23414 | Kalkunikintsuliha Rannam├Áisa kg                                | Rannam├Áisa     | meat_poultry              | {Maxima,Prisma,Selver}
 meat_poultry_turkey_duck            | 23415 | Kalkunikintsuliha r├Âstitud k├╝├╝slauguga Kikas 600g              | Kikas          | meat_poultry              | {Coop,Selver}
 meat_poultry_turkey_duck            | 23416 | Kalkunikintsuliha r├Âstitud paprika Kikas 600g                  | Kikas          | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 23417 | Kalkunikintsuliha aasiap. Kikas 600g                           | Kikas          | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 23418 | Kalkunikintsuliha BBQ apelsini marinaadis Kikas                | Kikas          | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 23419 | Kalkunikintsuliha kanepi-mee marinaadis Kikas                  | Kikas          | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 23420 | Kalkunikintsuliha rosmariini Kikas 700g                        | Kikas          | meat_poultry              | {Coop}
 meat_poultry_turkey_duck            | 23421 | Kalkunikintsuliha sinepi-olle marinaadis Kikas 700g            | Kikas          | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 23422 | Kalkun r├Âstitud k├╝├╝slaugu marinaadis Kikas 600g                | Kikas          | meat_poultry              | {Coop,Prisma}
 meat_poultry_turkey_duck            | 23424 | Kalkuni rinnafilee vurske Rimi kg                              | Rimi           | meat_poultry              | {Rimi}
 meat_poultry_turkey_duck            | 23425 | Kalkuni rinnafilee grill-liha Rannarootsi 400g                 | Rannarootsi    | meat_poultry              | {Selver}
 meat_poultry_turkey_duck            | 23426 | Kalkuniguljass Rannam├Áisa 400g                                 | Rannam├Áisa     | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 23428 | Kalkuni rinnafilee Rannam├Áisa kg duubl                         | Rannam├Áisa     | meat_poultry              | {Prisma,Selver}
 meat_poultry_turkey_duck            | 23430 | Kalkuni rinnafilee lougud WELL DONE 400g                       | Well Done      | meat_poultry              | {Maxima}
 meat_poultry_turkey_duck            | 23431 | Rannarootsi Roheliste Oliividega Kalkunipraad 600g             | Rannarootsi    | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 23432 | RM kalkunifilee 850g                                           | Rannam├Áisa     | meat_poultry              | {Coop}
 meat_poultry_turkey_duck            | 23433 | RM kalkunikintsuliha 1kg                                       | Rannam├Áisa     | meat_poultry              | {Coop}
 meat_poultry_turkey_duck            | 23434 | Jahutatud pardikoib Rannam├Áisa kg                              | Rannam├Áisa     | meat_poultry              | {Prisma,Selver}
 meat_poultry_turkey_duck            | 23435 | Jahutatud pardikoivad A-klass kg                               | Coop           | meat_poultry              | {Maxima}
 meat_poultry_turkey_duck            | 23438 | Pardi-confit Rannam├Áisa 500g                                   | Rannam├Áisa     | meat_poultry              | {Prisma,Rimi,Selver}
 meat_poultry_turkey_duck            | 23439 | Pardifilee v├╝rtsidega Kitchen Me 170g                          | Kitchen Me     | meat_poultry              | {Maxima,Rimi}
 meat_poultry_turkey_duck            | 23440 | Pardikoib Aasiap. Linnam├ñe 500g                                | Linnam├Áe       | meat_poultry              | {Rimi,Selver}
 meat_poultry_turkey_duck            | 23441 | Pardikoivad marineeritud WELL DONE kg                          | Well Done      | meat_poultry              | {Maxima}
 meat_poultry_turkey_duck            | 23442 | Pardikoivad vurske Reinuvaderi Pidusook kg                     | Reinuvaderi    | meat_poultry              | {Rimi}
 meat_poultry_turkey_duck            | 23443 | Pardi rinnafilee marineeritud WELL DONE kg                     | Well Done      | meat_poultry              | {Maxima}
 meat_poultry_turkey_duck            | 23444 | Pardirinnafilee nahaga vurske Reinuvaderi kg                   | Reinuvaderi    | meat_poultry              | {Rimi}
 meat_poultry_turkey_duck            | 23445 | Pardi rinnafilee Rannam├Áisa kg                                 | Rannam├Áisa     | meat_poultry              | {Selver}
 meat_poultry_turkey_duck            | 23447 | Pardi rinnafilee nahaga MARKUS MAREK kg                        | Markus Marek   | meat_poultry              | {Selver}
 meat_poultry_turkey_duck            | 23448 | Pekingi part 1/2 kondita r├Âstitud 600g                         | Coop           | meat_poultry              | {Coop}
 meat_poultry_turkey_duck            | 23450 | Linnam├ñe hrk peakoka pardirinnafilee 400g                      | Linnam├Áe       | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 23451 | Krobe meepart Rannam├Áisa kg                                    | Rannam├Áisa     | meat_poultry              | {Rimi}
 meat_poultry_turkey_duck            | 23454 | Rannam├Áisa pardi rinnafilee nahaga kg                          | Rannam├Áisa     | meat_poultry              | {Coop,Prisma,Selver}
 meat_poultry_turkey_duck            | 28211 | RM Part kg jahutatud                                           | Rannam├Áisa     | meat_poultry              | {Coop}
 meat_poultry_turkey_duck            | 50299 | Kana- ja kalkunikotletid praetud Salling 125g                  | Salling        | meat_poultry              | {Rimi}
 meat_poultry_turkey_duck            | 50305 | Kikas Kalkunikintsuliha itaaliapaarane marinaadis 600g         | Kikas          | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 50316 | Pardifilee marinaadis Linnamoe kg                              | Linnam├Áe       | meat_poultry              | {Selver}
 meat_poultry_turkey_duck            | 51987 | Rannam├Áisa Kalkuni rinnafileest grill-liha 400g                | Rannam├Áisa     | meat_poultry              | {Prisma}
 meat_poultry_turkey_duck            | 55795 | Kergsuitsu kalkunirinnafilee Rannam├Áisa kg                     | Rannam├Áisa     | meat_poultry              | {Maxima}
 meat_poultry_turkey_duck            | 55804 | Pekingi part WELL DONE kg                                      | Well Done      | meat_poultry              | {Maxima}
 meat_sausages_cross_species         | 44078 | Doktorivorst Well Done juustuga 300g                           | Well Done      | meat_sausages             | {Maxima}
 meat_sausages_cross_species         | 44079 | Doktorivorst M&M suitsutatud 500g                              | Maks & Moorits | meat_sausages             | {Coop,Maxima,Prisma,Selver}
 meat_sausages_cross_species         | 44080 | Doktorivorst M&M 600g                                          | Maks & Moorits | meat_sausages             | {Coop,Maxima,Prisma,Selver}
 meat_sausages_cross_species         | 44085 | Lastevorst Rakvere kg-lett                                     | Rakvere        | meat_sausages             | {Maxima,Selver}
 meat_sausages_cross_species         | 44087 | Lastevorst Rakvere Lihakas 170g viil                           | Rakvere        | meat_sausages             | {Coop,Maxima,Prisma,Selver}
 meat_sausages_cross_species         | 44077 | Doktorivorst Well Done 300g                                    | Well Done      | meat_sausages             | {Maxima}
 meat_sausages_cross_species         | 44076 | Doktorivorst Pormet 400g                                       | Pormet         | meat_sausages             | {Maxima}
 meat_sausages_cross_species         | 44075 | Doktorivorst Pormet 300g                                       | Pormet         | meat_sausages             | {Maxima}
 meat_sausages_cross_species         | 44073 | Doktorivorst UVIC kg                                           | UVIC           | meat_sausages             | {Maxima}
 meat_sausages_cross_species         | 44072 | Doktorivorst NoO kg-lett                                       | N├ÁO            | meat_sausages             | {Maxima,Selver}
 meat_sausages_cross_species         | 44071 | Doktorivorst Rakvere Lihakas 360g                              | Rakvere        | meat_sausages             | {Prisma,Selver}
 meat_sausages_cross_species         | 44070 | Doktorivorst Rakvere Lihakas 170g viil                         | Rakvere        | meat_sausages             | {Coop,Selver}
 meat_sausages_cross_species         | 44069 | Doktorivorst Rakvere 190g viil                                 | Rakvere        | meat_sausages             | {Coop,Maxima,Prisma,Rimi}
 meat_sausages_cross_species         | 44068 | Doktorivorst Rakvere kg-lett                                   | Rakvere        | meat_sausages             | {Maxima}
 meat_sausages_cross_species         | 12792 | Antu Gurmee mahekana viiner 350g                               | Antu Gurmee    | meat_sausages             | {Prisma}
 never_tested_subcodes               |  7627 | Semper pirni-aprikoosi puder 6k 120g                           | Semper         | baby_porridge_cereal      | {Coop,Prisma,Rimi}
 never_tested_subcodes               |  7617 | Ponn mitmeviljapuder ├Áuna-kaneeli oko 6k 110g                  | Ponn           | baby_porridge_cereal      | {Coop,Rimi,Selver}
 never_tested_subcodes               |  7618 | Ponn t├ñisterapuder banaani-mustsastraga oko 6k 200g            | Ponn           | baby_porridge_cereal      | {Coop,Prisma,Selver}
 never_tested_subcodes               |  7619 | Ponn teraviljapuder piima-puuviljadega HEAD UND oko 6k 110g    | Ponn           | baby_porridge_cereal      | {Coop,Rimi,Selver}
 never_tested_subcodes               |  7620 | Ponn hirsi-kaerapuder kollase ploomi-mangoga oko 6k 110g       | Ponn           | baby_porridge_cereal      | {Coop,Rimi,Selver}
 never_tested_subcodes               |  7621 | Semper kaerapuder ├Áuna-kaneeli 6k 120g                         | Semper         | baby_porridge_cereal      | {Coop}
 never_tested_subcodes               |  7622 | Semper mitmeviljapuder ├Áuna-maasika-mustika 8k 120g            | Semper         | baby_porridge_cereal      | {Coop,Maxima}
 never_tested_subcodes               |  7623 | Semper mitmeviljapuder ├Áuna-virsiku-aprikoos 8k 120g           | Semper         | baby_porridge_cereal      | {Coop}
 never_tested_subcodes               |  7624 | Semper maasika-banaanipuder 6k 120g                            | Semper         | baby_porridge_cereal      | {Coop,Prisma}
 never_tested_subcodes               |  7625 | Semper mustika-├Áunapuder 6k 120g                               | Semper         | baby_porridge_cereal      | {Prisma,Rimi}
 never_tested_subcodes               |  7626 | Semper ├Áuna-virsiku puder 6k 120g                              | Semper         | baby_porridge_cereal      | {Coop,Prisma}
 never_tested_subcodes               |  7597 | Kaerapudrupulber BIO 4k 200g                                   | Hipp           | baby_porridge_cereal      | {Coop,Prisma,Rimi}
 never_tested_subcodes               |  7598 | Riisipudrupulber BIO 4k 200g                                   | Hipp           | baby_porridge_cereal      | {Prisma,Rimi}
 never_tested_subcodes               |  7599 | Tatra piimapudrupulber 4k 250g                                 | Hipp           | baby_porridge_cereal      | {Coop,Prisma,Selver}
 never_tested_subcodes               |  7600 | Head Ood piimapuder banaaniga 4k 190g                          | Hipp           | baby_porridge_cereal      | {Coop,Prisma,Rimi,Selver}
 never_tested_subcodes               |  7601 | Head Ood piimapuder puuviljadega 4k 190g                       | Hipp           | baby_porridge_cereal      | {Coop,Selver}
 never_tested_subcodes               |  7602 | Head Ood piimapuder k├╝psistega 4k 190g                         | Hipp           | baby_porridge_cereal      | {Coop,Prisma,Rimi,Selver}
 never_tested_subcodes               |  7603 | Head Ood banaan-kuiviku piimapudrupulber 4k 250g               | Hipp           | baby_porridge_cereal      | {Coop,Prisma,Rimi,Selver}
 never_tested_subcodes               |  7604 | Head Ood k├╝psistega piimapudrupulber 6k 250g                   | Hipp           | baby_porridge_cereal      | {Coop,Selver}
 never_tested_subcodes               |  7605 | Head Ood kaera-├Áuna piimapudrupulber 6k 250g                   | Hipp           | baby_porridge_cereal      | {Coop,Prisma,Rimi,Selver}
 never_tested_subcodes               |  7606 | 5-vilja piimapuder ploomiga 6k 250g                            | Hipp           | baby_porridge_cereal      | {Coop,Prisma,Rimi}
 never_tested_subcodes               |  7607 | Maisi-puuviljapudrupulber 6k 250g                              | Hipp           | baby_porridge_cereal      | {Coop,Prisma}
 never_tested_subcodes               |  7608 | 8K piimapuder prebiootikumidega puuviljadega 250g              | Hipp           | baby_porridge_cereal      | {Coop}
 never_tested_subcodes               |  7609 | Puuviljad jogurtiga piimapudrupulber BIO 8k 250g               | Hipp           | baby_porridge_cereal      | {Prisma,Rimi}
 never_tested_subcodes               |  7610 | Mitmeviljapuder oko 6k 200g                                    | Hipp           | baby_porridge_cereal      | {Coop,Prisma,Selver}
 never_tested_subcodes               |  7611 | Ponn kaerapuder aprikoosiga oko 6k 200g                        | Ponn           | baby_porridge_cereal      | {Coop,Prisma,Selver}
 never_tested_subcodes               |  7612 | Ponn kaerapuder mustikaga oko 6k 190g                          | Ponn           | baby_porridge_cereal      | {Coop,Prisma,Rimi,Selver}
 never_tested_subcodes               |  7613 | Ponn kaerapuder ploomi-mustsosta-kookosega oko 6k 110g         | Ponn           | baby_porridge_cereal      | {Coop,Prisma,Rimi,Selver}
 never_tested_subcodes               |  7614 | Ponn neljaviljapuder banaani-mustikaga oko 6k 110g             | Ponn           | baby_porridge_cereal      | {Coop,Prisma,Rimi,Selver}
 never_tested_subcodes               |  7616 | Ponn neljaviljapuder vaarikaga oko 6k 200g                     | Ponn           | baby_porridge_cereal      | {Coop,Rimi,Selver}
 oils_olive_brand_diversity          | 14192 | Ekstra vaarioliivioli elinikon kalam 500ml                     | Elinikon       | oils_olive                | {Prisma}
 oils_olive_brand_diversity          | 14211 | Herkku Organic vaarioliivioli 250ml                            | Herkku         | oils_olive                | {Prisma}
 oils_olive_brand_diversity          | 14212 | Herkku Organic vaarioliivioli 500ml                            | Herkku         | oils_olive                | {Prisma}
 oils_olive_brand_diversity          | 14215 | La Espaniola EVO olipress 500ml                                | La Espanola    | oils_olive                | {Prisma}
 oils_olive_brand_diversity          | 14218 | Luglio ekstra vaarioliivioli Kreeka 500ml                      | Luglio         | oils_olive                | {Prisma}
 oils_olive_brand_diversity          | 14232 | OLIVITAL oliivijaagoli 1L                                      | OLIVITAL       | oils_olive                | {Maxima}
 oils_olive_brand_diversity          | 14233 | Colavita oliivoli essential 500ml                              | Colavita       | oils_olive                | {Prisma}
 oils_olive_brand_diversity          | 14234 | Colavita oliivoli extra virgin Kreeka 750ml                    | Colavita       | oils_olive                | {Prisma}
 oils_olive_brand_diversity          | 14235 | Colavita oliivoli extra virgin premium 750ml                   | Colavita       | oils_olive                | {Prisma}
 oils_olive_brand_diversity          | 14189 | Sidrunimaitsega oliivoli 250ml                                 | Coop           | oils_olive                | {Prisma}
(199 rows)
