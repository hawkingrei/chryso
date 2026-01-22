# Parser, Logical Plan, Physical Plan TODOs (500)

## Notes
- Keep dual parser tracks (yacc + handcrafted); benchmark and keep handcrafted path faster.
- Defer planner support for WITH/SET ops until parser coverage stabilizes.

## Parser (1-200)

1. Parser: lexer: add token for CASE
2. Parser: dialect: support CAST keyword parsing
3. Parser: expr: parse ILIKE operator precedence
4. Parser: expr: parse IS NULL expression form
5. Parser: literal: parse IS NOT NULL literal
6. Parser: identifier: support BETWEEN quoting rules
7. Parser: statement: parse NOT BETWEEN statement variant
8. Parser: select: support IN list clause
9. Parser: join: support NOT IN list join syntax
10. Parser: function: parse EXISTS function call
11. Parser: window: parse ANY/ALL window clause
12. Parser: predicate: normalize DISTINCT ON in parser
13. Parser: error: improve QUALIFY diagnostic
14. Parser: AST: add node for WITH CTE
15. Parser: AST: normalize RECURSIVE CTE during parse
16. Parser: parser: enable UNION feature flag
17. Parser: parser: add tests for UNION ALL
18. Parser: parser: improve recovery for EXCEPT
19. Parser: parser: support INTERSECT hints
20. Parser: parser: handle WINDOW frame comments
21. Parser: lexer: add token for ROWS
22. Parser: dialect: support RANGE keyword parsing
23. Parser: expr: parse GROUPS operator precedence
24. Parser: expr: parse OVER(PARTITION) expression form
25. Parser: literal: parse LATERAL literal
26. Parser: identifier: support CROSS JOIN quoting rules
27. Parser: statement: parse RIGHT JOIN statement variant
28. Parser: select: support FULL JOIN clause
29. Parser: join: support USING join syntax
30. Parser: function: parse NATURAL JOIN function call
31. Parser: window: parse LIMIT/OFFSET window clause
32. Parser: predicate: normalize FETCH FIRST in parser
33. Parser: error: improve TOP diagnostic
34. Parser: AST: add node for ORDER BY NULLS FIRST/LAST
35. Parser: AST: normalize NULLIF during parse
36. Parser: parser: enable COALESCE feature flag
37. Parser: parser: add tests for GREATEST/LEAST
38. Parser: parser: improve recovery for TRIM/UPPER/LOWER
39. Parser: parser: support ARRAY literal hints
40. Parser: parser: handle MAP literal comments
41. Parser: lexer: add token for STRUCT literal
42. Parser: dialect: support JSON literal keyword parsing
43. Parser: expr: parse INTERVAL literal operator precedence
44. Parser: expr: parse DATE/TIME/TIMESTAMP expression form
45. Parser: literal: parse BINARY/HEX literal literal
46. Parser: identifier: support ESCAPE in LIKE quoting rules
47. Parser: statement: parse POSITION/SUBSTRING statement variant
48. Parser: select: support REGEXP clause
49. Parser: join: support DELIMITED identifiers join syntax
50. Parser: function: parse schema-qualified identifiers function call
51. Parser: window: parse parameter placeholders window clause
52. Parser: predicate: normalize named parameters in parser
53. Parser: error: improve INSERT ... SELECT diagnostic
54. Parser: AST: add node for INSERT DEFAULT VALUES
55. Parser: AST: normalize UPSERT/MERGE during parse
56. Parser: parser: enable UPDATE ... FROM feature flag
57. Parser: parser: add tests for DELETE ... USING
58. Parser: parser: improve recovery for RETURNING
59. Parser: parser: support ALTER TABLE hints
60. Parser: parser: handle DROP TABLE comments
61. Parser: lexer: add token for CREATE INDEX
62. Parser: dialect: support ANALYZE TABLE keyword parsing
63. Parser: expr: parse EXPLAIN ANALYZE operator precedence
64. Parser: expr: parse SHOW expression form
65. Parser: literal: parse DESCRIBE literal
66. Parser: identifier: support mysql mode quoting rules
67. Parser: statement: parse pgsql mode statement variant
68. Parser: select: support CASE clause
69. Parser: join: support CAST join syntax
70. Parser: function: parse ILIKE function call
71. Parser: window: parse IS NULL window clause
72. Parser: predicate: normalize IS NOT NULL in parser
73. Parser: error: improve BETWEEN diagnostic
74. Parser: AST: add node for NOT BETWEEN
75. Parser: AST: normalize IN list during parse
76. Parser: parser: enable NOT IN list feature flag
77. Parser: parser: add tests for EXISTS
78. Parser: parser: improve recovery for ANY/ALL
79. Parser: parser: support DISTINCT ON hints
80. Parser: parser: handle QUALIFY comments
81. Parser: lexer: add token for WITH CTE
82. Parser: dialect: support RECURSIVE CTE keyword parsing
83. Parser: expr: parse UNION operator precedence
84. Parser: expr: parse UNION ALL expression form
85. Parser: literal: parse EXCEPT literal
86. Parser: identifier: support INTERSECT quoting rules
87. Parser: statement: parse WINDOW frame statement variant
88. Parser: select: support ROWS clause
89. Parser: join: support RANGE join syntax
90. Parser: function: parse GROUPS function call
91. Parser: window: parse OVER(PARTITION) window clause
92. Parser: predicate: normalize LATERAL in parser
93. Parser: error: improve CROSS JOIN diagnostic
94. Parser: AST: add node for RIGHT JOIN
95. Parser: AST: normalize FULL JOIN during parse
96. Parser: parser: enable USING feature flag
97. Parser: parser: add tests for NATURAL JOIN
98. Parser: parser: improve recovery for LIMIT/OFFSET
99. Parser: parser: support FETCH FIRST hints
100. Parser: parser: handle TOP comments
101. Parser: lexer: add token for ORDER BY NULLS FIRST/LAST
102. Parser: dialect: support NULLIF keyword parsing
103. Parser: expr: parse COALESCE operator precedence
104. Parser: expr: parse GREATEST/LEAST expression form
105. Parser: literal: parse TRIM/UPPER/LOWER literal
106. Parser: identifier: support ARRAY literal quoting rules
107. Parser: statement: parse MAP literal statement variant
108. Parser: select: support STRUCT literal clause
109. Parser: join: support JSON literal join syntax
110. Parser: function: parse INTERVAL literal function call
111. Parser: window: parse DATE/TIME/TIMESTAMP window clause
112. Parser: predicate: normalize BINARY/HEX literal in parser
113. Parser: error: improve ESCAPE in LIKE diagnostic
114. Parser: AST: add node for POSITION/SUBSTRING
115. Parser: AST: normalize REGEXP during parse
116. Parser: parser: enable DELIMITED identifiers feature flag
117. Parser: parser: add tests for schema-qualified identifiers
118. Parser: parser: improve recovery for parameter placeholders
119. Parser: parser: support named parameters hints
120. Parser: parser: handle INSERT ... SELECT comments
121. Parser: lexer: add token for INSERT DEFAULT VALUES
122. Parser: dialect: support UPSERT/MERGE keyword parsing
123. Parser: expr: parse UPDATE ... FROM operator precedence
124. Parser: expr: parse DELETE ... USING expression form
125. Parser: literal: parse RETURNING literal
126. Parser: identifier: support ALTER TABLE quoting rules
127. Parser: statement: parse DROP TABLE statement variant
128. Parser: select: support CREATE INDEX clause
129. Parser: join: support ANALYZE TABLE join syntax
130. Parser: function: parse EXPLAIN ANALYZE function call
131. Parser: window: parse SHOW window clause
132. Parser: predicate: normalize DESCRIBE in parser
133. Parser: error: improve mysql mode diagnostic
134. Parser: AST: add node for pgsql mode
135. Parser: AST: normalize CASE during parse
136. Parser: parser: enable CAST feature flag
137. Parser: parser: add tests for ILIKE
138. Parser: parser: improve recovery for IS NULL
139. Parser: parser: support IS NOT NULL hints
140. Parser: parser: handle BETWEEN comments
141. Parser: lexer: add token for NOT BETWEEN
142. Parser: dialect: support IN list keyword parsing
143. Parser: expr: parse NOT IN list operator precedence
144. Parser: expr: parse EXISTS expression form
145. Parser: literal: parse ANY/ALL literal
146. Parser: identifier: support DISTINCT ON quoting rules
147. Parser: statement: parse QUALIFY statement variant
148. Parser: select: support WITH CTE clause
149. Parser: join: support RECURSIVE CTE join syntax
150. Parser: function: parse UNION function call
151. Parser: window: parse UNION ALL window clause
152. Parser: predicate: normalize EXCEPT in parser
153. Parser: error: improve INTERSECT diagnostic
154. Parser: AST: add node for WINDOW frame
155. Parser: AST: normalize ROWS during parse
156. Parser: parser: enable RANGE feature flag
157. Parser: parser: add tests for GROUPS
158. Parser: parser: improve recovery for OVER(PARTITION)
159. Parser: parser: support LATERAL hints
160. Parser: parser: handle CROSS JOIN comments
161. Parser: lexer: add token for RIGHT JOIN
162. Parser: dialect: support FULL JOIN keyword parsing
163. Parser: expr: parse USING operator precedence
164. Parser: expr: parse NATURAL JOIN expression form
165. Parser: literal: parse LIMIT/OFFSET literal
166. Parser: identifier: support FETCH FIRST quoting rules
167. Parser: statement: parse TOP statement variant
168. Parser: select: support ORDER BY NULLS FIRST/LAST clause
169. Parser: join: support NULLIF join syntax
170. Parser: function: parse COALESCE function call
171. Parser: window: parse GREATEST/LEAST window clause
172. Parser: predicate: normalize TRIM/UPPER/LOWER in parser
173. Parser: error: improve ARRAY literal diagnostic
174. Parser: AST: add node for MAP literal
175. Parser: AST: normalize STRUCT literal during parse
176. Parser: parser: enable JSON literal feature flag
177. Parser: parser: add tests for INTERVAL literal
178. Parser: parser: improve recovery for DATE/TIME/TIMESTAMP
179. Parser: parser: support BINARY/HEX literal hints
180. Parser: parser: handle ESCAPE in LIKE comments
181. Parser: lexer: add token for POSITION/SUBSTRING
182. Parser: dialect: support REGEXP keyword parsing
183. Parser: expr: parse DELIMITED identifiers operator precedence
184. Parser: expr: parse schema-qualified identifiers expression form
185. Parser: literal: parse parameter placeholders literal
186. Parser: identifier: support named parameters quoting rules
187. Parser: statement: parse INSERT ... SELECT statement variant
188. Parser: select: support INSERT DEFAULT VALUES clause
189. Parser: join: support UPSERT/MERGE join syntax
190. Parser: function: parse UPDATE ... FROM function call
191. Parser: window: parse DELETE ... USING window clause
192. Parser: predicate: normalize RETURNING in parser
193. Parser: error: improve ALTER TABLE diagnostic
194. Parser: AST: add node for DROP TABLE
195. Parser: AST: normalize CREATE INDEX during parse
196. Parser: parser: enable ANALYZE TABLE feature flag
197. Parser: parser: add tests for EXPLAIN ANALYZE
198. Parser: parser: improve recovery for SHOW
199. Parser: parser: support DESCRIBE hints
200. Parser: parser: handle mysql mode comments

## Logical Plan (201-350)

201. Logical: rule: add predicate transitive closure rewrite
202. Logical: rule: add constant folding pushdown
203. Logical: rule: add nullability simplification
204. Logical: rule: add outer join simplification elimination
205. Logical: rule: add projection pruning normalization
206. Logical: plan: add node for filter pushdown
207. Logical: plan: enrich metadata for limit/offset pushdown
208. Logical: analysis: track top-n rewrite properties
209. Logical: analysis: derive join commutativity constraints
210. Logical: logical: infer join associativity typing
211. Logical: logical: resolve join reorder names
212. Logical: logical: validate semi-join reduction usage
213. Logical: logical: rewrite subquery decorrelation expressions
214. Logical: logical: ensure exists-to-join ordering
215. Logical: logical: add tests for in-to-join
216. Logical: rule: add aggregate pushdown rewrite
217. Logical: rule: add distinct elimination pushdown
218. Logical: rule: add group-by pruning simplification
219. Logical: rule: add having split elimination
220. Logical: rule: add sort elimination normalization
221. Logical: plan: add node for redundant filter removal
222. Logical: plan: enrich metadata for redundant projection removal
223. Logical: analysis: track common subexpression properties
224. Logical: analysis: derive expression canonicalization constraints
225. Logical: logical: infer CNF/DNF rewrite typing
226. Logical: logical: resolve domain inference names
227. Logical: logical: validate range propagation usage
228. Logical: logical: rewrite constraint propagation expressions
229. Logical: logical: ensure scalar subquery handling ordering
230. Logical: logical: add tests for window function placement
231. Logical: rule: add qualify handling rewrite
232. Logical: rule: add partition pruning pushdown
233. Logical: rule: add index selection hints simplification
234. Logical: rule: add cte inlining elimination
235. Logical: rule: add union all flattening normalization
236. Logical: plan: add node for set-op normalization
237. Logical: plan: enrich metadata for limit/offset normalization
238. Logical: analysis: track type coercion rules properties
239. Logical: analysis: derive collation handling constraints
240. Logical: logical: infer identifier qualification typing
241. Logical: logical: resolve cardinality estimation names
242. Logical: logical: validate stats usage usage
243. Logical: logical: rewrite cost annotations expressions
244. Logical: logical: ensure logical plan hashing ordering
245. Logical: logical: add tests for memo keys
246. Logical: rule: add rule conflicts rewrite
247. Logical: rule: add predicate transitive closure pushdown
248. Logical: rule: add constant folding simplification
249. Logical: rule: add nullability elimination
250. Logical: rule: add outer join simplification normalization
251. Logical: plan: add node for projection pruning
252. Logical: plan: enrich metadata for filter pushdown
253. Logical: analysis: track limit/offset pushdown properties
254. Logical: analysis: derive top-n rewrite constraints
255. Logical: logical: infer join commutativity typing
256. Logical: logical: resolve join associativity names
257. Logical: logical: validate join reorder usage
258. Logical: logical: rewrite semi-join reduction expressions
259. Logical: logical: ensure subquery decorrelation ordering
260. Logical: logical: add tests for exists-to-join
261. Logical: rule: add in-to-join rewrite
262. Logical: rule: add aggregate pushdown pushdown
263. Logical: rule: add distinct elimination simplification
264. Logical: rule: add group-by pruning elimination
265. Logical: rule: add having split normalization
266. Logical: plan: add node for sort elimination
267. Logical: plan: enrich metadata for redundant filter removal
268. Logical: analysis: track redundant projection removal properties
269. Logical: analysis: derive common subexpression constraints
270. Logical: logical: infer expression canonicalization typing
271. Logical: logical: resolve CNF/DNF rewrite names
272. Logical: logical: validate domain inference usage
273. Logical: logical: rewrite range propagation expressions
274. Logical: logical: ensure constraint propagation ordering
275. Logical: logical: add tests for scalar subquery handling
276. Logical: rule: add window function placement rewrite
277. Logical: rule: add qualify handling pushdown
278. Logical: rule: add partition pruning simplification
279. Logical: rule: add index selection hints elimination
280. Logical: rule: add cte inlining normalization
281. Logical: plan: add node for union all flattening
282. Logical: plan: enrich metadata for set-op normalization
283. Logical: analysis: track limit/offset normalization properties
284. Logical: analysis: derive type coercion rules constraints
285. Logical: logical: infer collation handling typing
286. Logical: logical: resolve identifier qualification names
287. Logical: logical: validate cardinality estimation usage
288. Logical: logical: rewrite stats usage expressions
289. Logical: logical: ensure cost annotations ordering
290. Logical: logical: add tests for logical plan hashing
291. Logical: rule: add memo keys rewrite
292. Logical: rule: add rule conflicts pushdown
293. Logical: rule: add predicate transitive closure simplification
294. Logical: rule: add constant folding elimination
295. Logical: rule: add nullability normalization
296. Logical: plan: add node for outer join simplification
297. Logical: plan: enrich metadata for projection pruning
298. Logical: analysis: track filter pushdown properties
299. Logical: analysis: derive limit/offset pushdown constraints
300. Logical: logical: infer top-n rewrite typing
301. Logical: logical: resolve join commutativity names
302. Logical: logical: validate join associativity usage
303. Logical: logical: rewrite join reorder expressions
304. Logical: logical: ensure semi-join reduction ordering
305. Logical: logical: add tests for subquery decorrelation
306. Logical: rule: add exists-to-join rewrite
307. Logical: rule: add in-to-join pushdown
308. Logical: rule: add aggregate pushdown simplification
309. Logical: rule: add distinct elimination elimination
310. Logical: rule: add group-by pruning normalization
311. Logical: plan: add node for having split
312. Logical: plan: enrich metadata for sort elimination
313. Logical: analysis: track redundant filter removal properties
314. Logical: analysis: derive redundant projection removal constraints
315. Logical: logical: infer common subexpression typing
316. Logical: logical: resolve expression canonicalization names
317. Logical: logical: validate CNF/DNF rewrite usage
318. Logical: logical: rewrite domain inference expressions
319. Logical: logical: ensure range propagation ordering
320. Logical: logical: add tests for constraint propagation
321. Logical: rule: add scalar subquery handling rewrite
322. Logical: rule: add window function placement pushdown
323. Logical: rule: add qualify handling simplification
324. Logical: rule: add partition pruning elimination
325. Logical: rule: add index selection hints normalization
326. Logical: plan: add node for cte inlining
327. Logical: plan: enrich metadata for union all flattening
328. Logical: analysis: track set-op normalization properties
329. Logical: analysis: derive limit/offset normalization constraints
330. Logical: logical: infer type coercion rules typing
331. Logical: logical: resolve collation handling names
332. Logical: logical: validate identifier qualification usage
333. Logical: logical: rewrite cardinality estimation expressions
334. Logical: logical: ensure stats usage ordering
335. Logical: logical: add tests for cost annotations
336. Logical: rule: add logical plan hashing rewrite
337. Logical: rule: add memo keys pushdown
338. Logical: rule: add rule conflicts simplification
339. Logical: rule: add predicate transitive closure elimination
340. Logical: rule: add constant folding normalization
341. Logical: plan: add node for nullability
342. Logical: plan: enrich metadata for outer join simplification
343. Logical: analysis: track projection pruning properties
344. Logical: analysis: derive filter pushdown constraints
345. Logical: logical: infer limit/offset pushdown typing
346. Logical: logical: resolve top-n rewrite names
347. Logical: logical: validate join commutativity usage
348. Logical: logical: rewrite join associativity expressions
349. Logical: logical: ensure join reorder ordering
350. Logical: logical: add tests for semi-join reduction

## Physical Plan (351-500)

351. Physical: physical: add hash join operator
352. Physical: physical: add nested loop join implementation
353. Physical: physical: add merge join cost model
354. Physical: physical: add index nested loop property enforcement
355. Physical: physical: add streaming aggregate distribution
356. Physical: physical: add hash aggregate partitioning
357. Physical: physical: add sort aggregate exchange
358. Physical: physical: add top-n spill strategy
359. Physical: physical: add external sort vectorization
360. Physical: physical: add limit predicate evaluation
361. Physical: physical: add offset join algorithm
362. Physical: physical: add table scan aggregation algorithm
363. Physical: physical: add index scan sorting algorithm
364. Physical: physical: add bitmap scan execution hint
365. Physical: physical: add tests for projection
366. Physical: physical: add filter operator
367. Physical: physical: add runtime filter implementation
368. Physical: physical: add data exchange cost model
369. Physical: physical: add repartition property enforcement
370. Physical: physical: add broadcast distribution
371. Physical: physical: add gather partitioning
372. Physical: physical: add pipeline breaker exchange
373. Physical: physical: add materialization spill strategy
374. Physical: physical: add early projection vectorization
375. Physical: physical: add predicate pushdown predicate evaluation
376. Physical: physical: add late materialization join algorithm
377. Physical: physical: add codegen aggregation algorithm
378. Physical: physical: add vectorized execution sorting algorithm
379. Physical: physical: add spill-to-disk execution hint
380. Physical: physical: add tests for memory budget enforcement
381. Physical: physical: add row format operator
382. Physical: physical: add columnar format implementation
383. Physical: physical: add operator fusion cost model
384. Physical: physical: add join reordering property enforcement
385. Physical: physical: add join selection distribution
386. Physical: physical: add parallelism partitioning
387. Physical: physical: add task scheduling exchange
388. Physical: physical: add statistics feedback spill strategy
389. Physical: physical: add adaptive execution vectorization
390. Physical: physical: add runtime join reorder predicate evaluation
391. Physical: physical: add runtime re-optimization join algorithm
392. Physical: physical: add execution profiling aggregation algorithm
393. Physical: physical: add metrics export sorting algorithm
394. Physical: physical: add hash join execution hint
395. Physical: physical: add tests for nested loop join
396. Physical: physical: add merge join operator
397. Physical: physical: add index nested loop implementation
398. Physical: physical: add streaming aggregate cost model
399. Physical: physical: add hash aggregate property enforcement
400. Physical: physical: add sort aggregate distribution
401. Physical: physical: add top-n partitioning
402. Physical: physical: add external sort exchange
403. Physical: physical: add limit spill strategy
404. Physical: physical: add offset vectorization
405. Physical: physical: add table scan predicate evaluation
406. Physical: physical: add index scan join algorithm
407. Physical: physical: add bitmap scan aggregation algorithm
408. Physical: physical: add projection sorting algorithm
409. Physical: physical: add filter execution hint
410. Physical: physical: add tests for runtime filter
411. Physical: physical: add data exchange operator
412. Physical: physical: add repartition implementation
413. Physical: physical: add broadcast cost model
414. Physical: physical: add gather property enforcement
415. Physical: physical: add pipeline breaker distribution
416. Physical: physical: add materialization partitioning
417. Physical: physical: add early projection exchange
418. Physical: physical: add predicate pushdown spill strategy
419. Physical: physical: add late materialization vectorization
420. Physical: physical: add codegen predicate evaluation
421. Physical: physical: add vectorized execution join algorithm
422. Physical: physical: add spill-to-disk aggregation algorithm
423. Physical: physical: add memory budget enforcement sorting algorithm
424. Physical: physical: add row format execution hint
425. Physical: physical: add tests for columnar format
426. Physical: physical: add operator fusion operator
427. Physical: physical: add join reordering implementation
428. Physical: physical: add join selection cost model
429. Physical: physical: add parallelism property enforcement
430. Physical: physical: add task scheduling distribution
431. Physical: physical: add statistics feedback partitioning
432. Physical: physical: add adaptive execution exchange
433. Physical: physical: add runtime join reorder spill strategy
434. Physical: physical: add runtime re-optimization vectorization
435. Physical: physical: add execution profiling predicate evaluation
436. Physical: physical: add metrics export join algorithm
437. Physical: physical: add hash join aggregation algorithm
438. Physical: physical: add nested loop join sorting algorithm
439. Physical: physical: add merge join execution hint
440. Physical: physical: add tests for index nested loop
441. Physical: physical: add streaming aggregate operator
442. Physical: physical: add hash aggregate implementation
443. Physical: physical: add sort aggregate cost model
444. Physical: physical: add top-n property enforcement
445. Physical: physical: add external sort distribution
446. Physical: physical: add limit partitioning
447. Physical: physical: add offset exchange
448. Physical: physical: add table scan spill strategy
449. Physical: physical: add index scan vectorization
450. Physical: physical: add bitmap scan predicate evaluation
451. Physical: physical: add projection join algorithm
452. Physical: physical: add filter aggregation algorithm
453. Physical: physical: add runtime filter sorting algorithm
454. Physical: physical: add data exchange execution hint
455. Physical: physical: add tests for repartition
456. Physical: physical: add broadcast operator
457. Physical: physical: add gather implementation
458. Physical: physical: add pipeline breaker cost model
459. Physical: physical: add materialization property enforcement
460. Physical: physical: add early projection distribution
461. Physical: physical: add predicate pushdown partitioning
462. Physical: physical: add late materialization exchange
463. Physical: physical: add codegen spill strategy
464. Physical: physical: add vectorized execution vectorization
465. Physical: physical: add spill-to-disk predicate evaluation
466. Physical: physical: add memory budget enforcement join algorithm
467. Physical: physical: add row format aggregation algorithm
468. Physical: physical: add columnar format sorting algorithm
469. Physical: physical: add operator fusion execution hint
470. Physical: physical: add tests for join reordering
471. Physical: physical: add join selection operator
472. Physical: physical: add parallelism implementation
473. Physical: physical: add task scheduling cost model
474. Physical: physical: add statistics feedback property enforcement
475. Physical: physical: add adaptive execution distribution
476. Physical: physical: add runtime join reorder partitioning
477. Physical: physical: add runtime re-optimization exchange
478. Physical: physical: add execution profiling spill strategy
479. Physical: physical: add metrics export vectorization
480. Physical: physical: add hash join predicate evaluation
481. Physical: physical: add nested loop join join algorithm
482. Physical: physical: add merge join aggregation algorithm
483. Physical: physical: add index nested loop sorting algorithm
484. Physical: physical: add streaming aggregate execution hint
485. Physical: physical: add tests for hash aggregate
486. Physical: physical: add sort aggregate operator
487. Physical: physical: add top-n implementation
488. Physical: physical: add external sort cost model
489. Physical: physical: add limit property enforcement
490. Physical: physical: add offset distribution
491. Physical: physical: add table scan partitioning
492. Physical: physical: add index scan exchange
493. Physical: physical: add bitmap scan spill strategy
494. Physical: physical: add projection vectorization
495. Physical: physical: add filter predicate evaluation
496. Physical: physical: add runtime filter join algorithm
497. Physical: physical: add data exchange aggregation algorithm
498. Physical: physical: add repartition sorting algorithm
499. Physical: physical: add broadcast execution hint
500. Physical: physical: add tests for gather
