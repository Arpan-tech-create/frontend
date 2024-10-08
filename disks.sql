PGDMP      2                |            pool_management    16.3    16.3     �           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                      false            �           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                      false            �           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                      false            �           1262    27522    pool_management    DATABASE     �   CREATE DATABASE pool_management WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'English_India.1252';
    DROP DATABASE pool_management;
                postgres    false            �            1259    27558    disks    TABLE     �   CREATE TABLE public.disks (
    disk_id integer NOT NULL,
    pool_id_req integer NOT NULL,
    disk_name character varying(255) NOT NULL,
    capacity numeric NOT NULL,
    usage numeric NOT NULL,
    type character varying
);
    DROP TABLE public.disks;
       public         heap    postgres    false            �            1259    27557    disks_disk_id_seq    SEQUENCE     �   CREATE SEQUENCE public.disks_disk_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
 (   DROP SEQUENCE public.disks_disk_id_seq;
       public          postgres    false    218            �           0    0    disks_disk_id_seq    SEQUENCE OWNED BY     G   ALTER SEQUENCE public.disks_disk_id_seq OWNED BY public.disks.disk_id;
          public          postgres    false    217                       2604    27561    disks disk_id    DEFAULT     n   ALTER TABLE ONLY public.disks ALTER COLUMN disk_id SET DEFAULT nextval('public.disks_disk_id_seq'::regclass);
 <   ALTER TABLE public.disks ALTER COLUMN disk_id DROP DEFAULT;
       public          postgres    false    217    218    218            �          0    27558    disks 
   TABLE DATA           W   COPY public.disks (disk_id, pool_id_req, disk_name, capacity, usage, type) FROM stdin;
    public          postgres    false    218   �       �           0    0    disks_disk_id_seq    SEQUENCE SET     ?   SELECT pg_catalog.setval('public.disks_disk_id_seq', 4, true);
          public          postgres    false    217                       2606    27565    disks disks_pkey 
   CONSTRAINT     S   ALTER TABLE ONLY public.disks
    ADD CONSTRAINT disks_pkey PRIMARY KEY (disk_id);
 :   ALTER TABLE ONLY public.disks DROP CONSTRAINT disks_pkey;
       public            postgres    false    218                        2606    27566    disks disks_pool_id_req_fkey    FK CONSTRAINT     �   ALTER TABLE ONLY public.disks
    ADD CONSTRAINT disks_pool_id_req_fkey FOREIGN KEY (pool_id_req) REFERENCES public.pool(pool_id);
 F   ALTER TABLE ONLY public.disks DROP CONSTRAINT disks_pool_id_req_fkey;
       public          postgres    false    218            �   f   x�E̻�0E��y
&@��[�1hR�@@C�ȁ�,]��`,[�'�7�ʑ�\H�RSS�w�r�<�&��ɮ,��1b�C�O�@��oN��El����R&�     