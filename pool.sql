PGDMP  ,    2                |            pool_management    16.3    16.3     �           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                      false            �           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                      false            �           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                      false            �           1262    27522    pool_management    DATABASE     �   CREATE DATABASE pool_management WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'English_India.1252';
    DROP DATABASE pool_management;
                postgres    false            �            1259    27551    pool    TABLE     j   CREATE TABLE public.pool (
    pool_id integer NOT NULL,
    pool_name character varying(255) NOT NULL
);
    DROP TABLE public.pool;
       public         heap    postgres    false            �            1259    27550    pool_pool_id_seq    SEQUENCE     �   CREATE SEQUENCE public.pool_pool_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
 '   DROP SEQUENCE public.pool_pool_id_seq;
       public          postgres    false    216            �           0    0    pool_pool_id_seq    SEQUENCE OWNED BY     E   ALTER SEQUENCE public.pool_pool_id_seq OWNED BY public.pool.pool_id;
          public          postgres    false    215                       2604    27554    pool pool_id    DEFAULT     l   ALTER TABLE ONLY public.pool ALTER COLUMN pool_id SET DEFAULT nextval('public.pool_pool_id_seq'::regclass);
 ;   ALTER TABLE public.pool ALTER COLUMN pool_id DROP DEFAULT;
       public          postgres    false    215    216    216            �          0    27551    pool 
   TABLE DATA           2   COPY public.pool (pool_id, pool_name) FROM stdin;
    public          postgres    false    216   j
       �           0    0    pool_pool_id_seq    SEQUENCE SET     >   SELECT pg_catalog.setval('public.pool_pool_id_seq', 4, true);
          public          postgres    false    215                       2606    27556    pool pool_pkey 
   CONSTRAINT     Q   ALTER TABLE ONLY public.pool
    ADD CONSTRAINT pool_pkey PRIMARY KEY (pool_id);
 8   ALTER TABLE ONLY public.pool DROP CONSTRAINT pool_pkey;
       public            postgres    false    216            �   '   x�3����1�2�F\�`ژ�L�p��iS�=... 
�     