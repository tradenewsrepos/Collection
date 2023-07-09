CREATE TABLE public.trade_news_events (
	id uuid NOT NULL,
	classes _text NULL,
	itc_codes text NULL,
	locations text NULL,
	title text NULL,
	url text NULL,
	dates text NULL,
	article_ids _text NULL,
	product text NULL,
	status text NULL,
	CONSTRAINT trade_news_events_pkey PRIMARY KEY (id)
);