CREATE TABLE IF NOT EXISTS public.email_thread_summaries (
  thread_id INTEGER NOT NULL,
  summary TEXT NOT NULL,
  PRIMARY KEY (thread_id)
);
