-- The three Moss tables of the wagon hitobito_wsjrdp_2027, schema only: what
-- test_import_moss_transactions.py creates in the integration-testing DB.
-- Generated with: docker exec development-postgres-1 pg_dump -U hitobito -d
-- hitobito_development --schema-only --no-owner --no-privileges --no-comments
-- -t moss_transactions -t moss_expenses -t moss_bookings -- then stripped of the
-- SET / \restrict / set_config('search_path') lines and of the three FKs leaving
-- this set (-> datev_bookings, -> wsjrdp_camt_transactions); everything else,
-- internal FKs included, is verbatim. REGENERATE after a wagon migration here.

--
-- Name: moss_bookings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.moss_bookings (
    id bigint NOT NULL,
    created_at timestamp(6) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp(6) without time zone,
    moss_transaction_id bigint NOT NULL,
    moss_expense_id bigint NOT NULL,
    signed_base_amount numeric(20,3) NOT NULL,
    base_amount numeric(20,3) GENERATED ALWAYS AS (abs(signed_base_amount)) STORED,
    signed_transaction_amount numeric(20,3),
    transaction_amount numeric(20,3) GENERATED ALWAYS AS (abs(signed_transaction_amount)) STORED,
    debit_credit character varying GENERATED ALWAYS AS (
CASE
    WHEN (signed_base_amount > (0)::numeric) THEN 'C'::text
    ELSE 'D'::text
END) STORED,
    account_number character varying,
    account_kind character varying,
    account_type character varying GENERATED ALWAYS AS (
CASE
    WHEN (account_kind IS NULL) THEN NULL::text
    WHEN ((account_kind)::text = ANY (ARRAY[('CREDITOR'::character varying)::text, ('DEBITOR'::character varying)::text])) THEN 'WsjrdpPersonalAccount'::text
    ELSE 'WsjrdpLedgerAccount'::text
END) STORED,
    cost_center_number character varying,
    sphere_number character varying,
    distribution_combination character varying,
    booking_posting_text character varying DEFAULT ''::character varying NOT NULL,
    expense_datev_booking_id bigint,
    expense_datev_booking_link_meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    contribution_subject_id bigint,
    contribution_subject_type character varying,
    other_moss_columns jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_file character varying,
    comment text DEFAULT ''::text NOT NULL,
    additional_info jsonb DEFAULT '{}'::jsonb NOT NULL,
    sub_row_number integer NOT NULL,
    CONSTRAINT chk_moss_bookings_account_kind CHECK (((account_kind IS NULL) OR ((account_kind)::text = ANY (ARRAY[('BANK'::character varying)::text, ('TRANSIT'::character varying)::text, ('CLEARING'::character varying)::text, ('LIABILITY'::character varying)::text, ('CREDITOR'::character varying)::text, ('DEBITOR'::character varying)::text, ('INCOME'::character varying)::text, ('EXPENSE'::character varying)::text, ('EQUITY'::character varying)::text, ('UNKNOWN'::character varying)::text]))))
);


--
-- Name: moss_bookings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.moss_bookings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: moss_bookings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.moss_bookings_id_seq OWNED BY public.moss_bookings.id;


--
-- Name: moss_expenses; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.moss_expenses (
    id bigint NOT NULL,
    created_at timestamp(6) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp(6) without time zone,
    moss_transaction_id bigint NOT NULL,
    type character varying NOT NULL,
    moss_expense_uuid uuid NOT NULL,
    expense_number integer DEFAULT 1 NOT NULL,
    signed_expense_base_amount numeric(20,3) NOT NULL,
    expense_base_amount numeric(20,3) GENERATED ALWAYS AS (abs(signed_expense_base_amount)) STORED,
    signed_expense_transaction_amount numeric(20,3),
    expense_transaction_amount numeric(20,3) GENERATED ALWAYS AS (abs(signed_expense_transaction_amount)) STORED,
    expense_posting_text character varying,
    expense_name character varying,
    moss_expense_type character varying,
    purchased_on date,
    other_moss_columns jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_file character varying,
    comment text DEFAULT ''::text NOT NULL,
    additional_info jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: moss_expenses_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.moss_expenses_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: moss_expenses_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.moss_expenses_id_seq OWNED BY public.moss_expenses.id;


--
-- Name: moss_transactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.moss_transactions (
    id bigint NOT NULL,
    created_at timestamp(6) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp(6) without time zone,
    type character varying NOT NULL,
    expense_type character varying GENERATED ALWAYS AS (
CASE type
    WHEN 'MossCardTransaction'::text THEN 'card_transaction'::text
    WHEN 'MossInvoice'::text THEN 'invoice'::text
    WHEN 'MossReimbursement'::text THEN 'reimbursement'::text
    ELSE 'top_up'::text
END) STORED,
    moss_transaction_uuid uuid NOT NULL,
    moss_transaction_state character varying,
    status character varying,
    transaction_type character varying,
    payment_date date,
    booking_date date,
    first_export_date date,
    last_export_date date,
    settlement_date date,
    receipt_date date,
    service_date date,
    approval_date date,
    invoice_date date,
    invoice_status character varying,
    due_date date,
    delivery_date date,
    submitted_date date,
    created_in_moss_on date,
    submitted_on date,
    signed_total_base_amount numeric(20,3) NOT NULL,
    total_base_amount numeric(20,3) GENERATED ALWAYS AS (abs(signed_total_base_amount)) STORED,
    signed_total_transaction_amount numeric(20,3),
    total_transaction_amount numeric(20,3) GENERATED ALWAYS AS (abs(signed_total_transaction_amount)) STORED,
    currency character varying,
    currency_original character varying,
    exchange_rate numeric(28,12),
    payment_fee numeric(20,3),
    fees_amount numeric(20,3),
    total_amount_excluding_fees numeric(20,3),
    conversion_rate_including_fees numeric(28,12),
    supplier_account_number character varying,
    supplier_account_kind character varying,
    supplier_account_type character varying GENERATED ALWAYS AS (
CASE
    WHEN (supplier_account_kind IS NULL) THEN NULL::text
    WHEN ((supplier_account_kind)::text = ANY (ARRAY[('CREDITOR'::character varying)::text, ('DEBITOR'::character varying)::text])) THEN 'WsjrdpPersonalAccount'::text
    ELSE 'WsjrdpLedgerAccount'::text
END) STORED,
    recipient_iban character varying,
    recipient_bic character varying,
    recipient_name character varying,
    top_up_sender character varying,
    moss_balance_account_number character varying,
    moss_balance_account_kind character varying,
    moss_balance_account_type character varying GENERATED ALWAYS AS (
CASE
    WHEN (moss_balance_account_kind IS NULL) THEN NULL::text
    WHEN ((moss_balance_account_kind)::text = ANY (ARRAY[('CREDITOR'::character varying)::text, ('DEBITOR'::character varying)::text])) THEN 'WsjrdpPersonalAccount'::text
    ELSE 'WsjrdpLedgerAccount'::text
END) STORED,
    cash_in_transit_account_number character varying,
    cash_in_transit_account_kind character varying,
    cash_in_transit_account_type character varying GENERATED ALWAYS AS (
CASE
    WHEN (cash_in_transit_account_kind IS NULL) THEN NULL::text
    WHEN ((cash_in_transit_account_kind)::text = ANY (ARRAY[('CREDITOR'::character varying)::text, ('DEBITOR'::character varying)::text])) THEN 'WsjrdpPersonalAccount'::text
    ELSE 'WsjrdpLedgerAccount'::text
END) STORED,
    merchant_name character varying,
    merchant_city character varying,
    merchant_country character varying,
    card_holder_name character varying,
    card_holder_team_name character varying,
    card_used character varying,
    card_purpose character varying,
    approver_name character varying,
    post_spend_approval_status character varying,
    payout_user_name character varying,
    payout_team_name character varying,
    transaction_posting_text character varying DEFAULT ''::character varying NOT NULL,
    payment_reference character varying,
    transaction_name character varying,
    invoice_number character varying,
    po_number character varying,
    pr_number character varying,
    submitted_by character varying,
    moss_reimbursement_uuid uuid,
    moss_invoice_uuid uuid,
    fin_account_id bigint,
    recipient_id bigint,
    recipient_link_meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    clearing_datev_booking_id bigint,
    clearing_datev_booking_link_meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    camt_transaction_id bigint,
    camt_transaction_link_meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    manually_paid boolean DEFAULT false NOT NULL,
    manually_booked boolean DEFAULT false NOT NULL,
    other_moss_columns jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_file character varying,
    comment text DEFAULT ''::text NOT NULL,
    additional_info jsonb DEFAULT '{}'::jsonb NOT NULL,
    all_moss_transaction_uuids uuid[] DEFAULT '{}'::uuid[] NOT NULL,
    sender_iban character varying,
    sender_bic character varying,
    sender_name character varying,
    value_date date,
    moss_object_uuid uuid GENERATED ALWAYS AS (COALESCE(moss_reimbursement_uuid, moss_invoice_uuid, moss_transaction_uuid)) STORED NOT NULL,
    CONSTRAINT chk_moss_transactions_cash_in_transit_account_kind CHECK (((cash_in_transit_account_kind IS NULL) OR ((cash_in_transit_account_kind)::text = ANY (ARRAY[('BANK'::character varying)::text, ('TRANSIT'::character varying)::text, ('CLEARING'::character varying)::text, ('LIABILITY'::character varying)::text, ('CREDITOR'::character varying)::text, ('DEBITOR'::character varying)::text, ('INCOME'::character varying)::text, ('EXPENSE'::character varying)::text, ('EQUITY'::character varying)::text, ('UNKNOWN'::character varying)::text])))),
    CONSTRAINT chk_moss_transactions_moss_balance_account_kind CHECK (((moss_balance_account_kind IS NULL) OR ((moss_balance_account_kind)::text = ANY (ARRAY[('BANK'::character varying)::text, ('TRANSIT'::character varying)::text, ('CLEARING'::character varying)::text, ('LIABILITY'::character varying)::text, ('CREDITOR'::character varying)::text, ('DEBITOR'::character varying)::text, ('INCOME'::character varying)::text, ('EXPENSE'::character varying)::text, ('EQUITY'::character varying)::text, ('UNKNOWN'::character varying)::text])))),
    CONSTRAINT chk_moss_transactions_supplier_account_kind CHECK (((supplier_account_kind IS NULL) OR ((supplier_account_kind)::text = ANY (ARRAY[('BANK'::character varying)::text, ('TRANSIT'::character varying)::text, ('CLEARING'::character varying)::text, ('LIABILITY'::character varying)::text, ('CREDITOR'::character varying)::text, ('DEBITOR'::character varying)::text, ('INCOME'::character varying)::text, ('EXPENSE'::character varying)::text, ('EQUITY'::character varying)::text, ('UNKNOWN'::character varying)::text])))),
    CONSTRAINT chk_moss_transactions_type CHECK (((type)::text = ANY (ARRAY[('MossCardTransaction'::character varying)::text, ('MossInvoice'::character varying)::text, ('MossReimbursement'::character varying)::text, ('MossTopUp'::character varying)::text])))
);


--
-- Name: moss_transactions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.moss_transactions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: moss_transactions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.moss_transactions_id_seq OWNED BY public.moss_transactions.id;


--
-- Name: moss_bookings id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.moss_bookings ALTER COLUMN id SET DEFAULT nextval('public.moss_bookings_id_seq'::regclass);


--
-- Name: moss_expenses id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.moss_expenses ALTER COLUMN id SET DEFAULT nextval('public.moss_expenses_id_seq'::regclass);


--
-- Name: moss_transactions id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.moss_transactions ALTER COLUMN id SET DEFAULT nextval('public.moss_transactions_id_seq'::regclass);


--
-- Name: moss_bookings moss_bookings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.moss_bookings
    ADD CONSTRAINT moss_bookings_pkey PRIMARY KEY (id);


--
-- Name: moss_expenses moss_expenses_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.moss_expenses
    ADD CONSTRAINT moss_expenses_pkey PRIMARY KEY (id);


--
-- Name: moss_transactions moss_transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.moss_transactions
    ADD CONSTRAINT moss_transactions_pkey PRIMARY KEY (id);


--
-- Name: index_moss_bookings_account_number; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_bookings_account_number ON public.moss_bookings USING btree (account_number);


--
-- Name: index_moss_bookings_base_amount; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_bookings_base_amount ON public.moss_bookings USING btree (base_amount);


--
-- Name: index_moss_bookings_contribution_subject; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_bookings_contribution_subject ON public.moss_bookings USING btree (contribution_subject_type, contribution_subject_id);


--
-- Name: index_moss_bookings_expense; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_bookings_expense ON public.moss_bookings USING btree (moss_expense_id);


--
-- Name: index_moss_bookings_expense_datev; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_bookings_expense_datev ON public.moss_bookings USING btree (expense_datev_booking_id);


--
-- Name: index_moss_bookings_expense_sub_row; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX index_moss_bookings_expense_sub_row ON public.moss_bookings USING btree (moss_expense_id, sub_row_number);


--
-- Name: index_moss_bookings_transaction; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_bookings_transaction ON public.moss_bookings USING btree (moss_transaction_id);


--
-- Name: index_moss_expenses_expense_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX index_moss_expenses_expense_uuid ON public.moss_expenses USING btree (moss_expense_uuid);


--
-- Name: index_moss_expenses_transaction; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_expenses_transaction ON public.moss_expenses USING btree (moss_transaction_id);


--
-- Name: index_moss_expenses_transaction_expense_number; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX index_moss_expenses_transaction_expense_number ON public.moss_expenses USING btree (moss_transaction_id, expense_number);


--
-- Name: index_moss_expenses_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_expenses_type ON public.moss_expenses USING btree (type);


--
-- Name: index_moss_transactions_all_uuids; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_transactions_all_uuids ON public.moss_transactions USING gin (all_moss_transaction_uuids);


--
-- Name: index_moss_transactions_camt; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_transactions_camt ON public.moss_transactions USING btree (camt_transaction_id);


--
-- Name: index_moss_transactions_clearing_datev; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_transactions_clearing_datev ON public.moss_transactions USING btree (clearing_datev_booking_id);


--
-- Name: index_moss_transactions_invoice_number; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_transactions_invoice_number ON public.moss_transactions USING btree (invoice_number);


--
-- Name: index_moss_transactions_invoice_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX index_moss_transactions_invoice_uuid ON public.moss_transactions USING btree (moss_invoice_uuid) WHERE (moss_invoice_uuid IS NOT NULL);


--
-- Name: index_moss_transactions_object_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX index_moss_transactions_object_uuid ON public.moss_transactions USING btree (moss_object_uuid);


--
-- Name: index_moss_transactions_payment_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_transactions_payment_date ON public.moss_transactions USING btree (payment_date);


--
-- Name: index_moss_transactions_recipient; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_transactions_recipient ON public.moss_transactions USING btree (recipient_id);


--
-- Name: index_moss_transactions_reimbursement_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX index_moss_transactions_reimbursement_uuid ON public.moss_transactions USING btree (moss_reimbursement_uuid) WHERE (moss_reimbursement_uuid IS NOT NULL);


--
-- Name: index_moss_transactions_total_base_amount; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_transactions_total_base_amount ON public.moss_transactions USING btree (total_base_amount);


--
-- Name: index_moss_transactions_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_moss_transactions_type ON public.moss_transactions USING btree (type);


--
-- Name: index_moss_transactions_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX index_moss_transactions_uuid ON public.moss_transactions USING btree (moss_transaction_uuid);


--
-- Name: moss_expenses fk_rails_bab4150211; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.moss_expenses
    ADD CONSTRAINT fk_rails_bab4150211 FOREIGN KEY (moss_transaction_id) REFERENCES public.moss_transactions(id) ON DELETE CASCADE;


--
-- Name: moss_bookings fk_rails_bd393735b2; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.moss_bookings
    ADD CONSTRAINT fk_rails_bd393735b2 FOREIGN KEY (moss_expense_id) REFERENCES public.moss_expenses(id) ON DELETE CASCADE;


--
-- Name: moss_bookings fk_rails_c4e7d5c432; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.moss_bookings
    ADD CONSTRAINT fk_rails_c4e7d5c432 FOREIGN KEY (moss_transaction_id) REFERENCES public.moss_transactions(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--
