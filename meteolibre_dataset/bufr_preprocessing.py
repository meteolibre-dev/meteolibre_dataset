import math
import numpy as np
import pandas as pd

import os
import gzip

import matplotlib.pyplot as plt
from matplotlib import colors
from matplotlib import cm
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import scipy.io


liste_radar = {36 : 'NOYAL', 37 : 'AJACCIO', 38 : 'ST-REMY', 40 : 'ABBEVILLE', 41 : 'BORDEAUX', 42 : 'BOURGES', 43 : 'MOUCHEROTTE', 44 : 'BRIVE GREZES', 45 : 'FALAISE CAEN' , 47 : 'RADAR NANCY',
               49 : 'RADAR NIMES', 50 : 'TOULOUSE', 51 : 'TRAPPES', 52 : 'ARCIS TROYES', 53 : 'SEMBADEL', 54 : 'TREILLIERES', 55 : 'BOLLENE', 56 : 'PLABENNEC', 57 : 'OPOUL', 58 : 'ST.NIZIER',
               59 : 'COLLOBRIERES', 60 : 'VARS', 61 : 'ALERIA', 62 : 'MONTCLAR', 63 : "L'AVESNOIS", 64 : 'CHERVES', 65 : 'BLAISY-HAUT', 66 : 'MOMUY', 67 : 'MONTANCY', 68 : 'MAUREL',
               69 : 'COLOMBIS', 90 : 'GUADELOUPE LE MOULE', 91 : 'MARTINIQUE', 92 : 'LA RÉUNION COLORADO', 93: 'LA REUNION PITON VILLERS', 94 : 'NOUVELLE-CALÉDONIE NOUMEA', 96 : 'NOUVELLE-CALÉDONIE LIFOU'}


class BitReader(object):
    # to read bit by bit (and not only byte by byte)
    def __init__(self, f):
        self.input = f
        self.accumulator = 0
        self.bcount = 0
        self.read = 0
        self.total_read = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _readbit(self):
        if not self.bcount:
            a = self.input.read(1)
            if a:
                self.accumulator = ord(a)
            self.bcount = 8
            self.read = len(a)
        rv = (self.accumulator & (1 << self.bcount-1)) >> self.bcount-1
        self.bcount -= 1
        return rv

    def readbits(self, n):
        self.total_read += 1
        v = 0
        while n > 0:
            v = (v << 1) | self._readbit()
            n -= 1
        return v

def bits2bytes(chaine):
    ent = int(chaine,2)
    byte_number = ent.bit_length()
    bin_array= ent.to_bytes(byte_number, "big")
    bin_array = bin_array.strip(b'\0')
    return bin_array.decode()

# to decode the real 'F' and 'X' value of a descriptor
def bytes_desc(byt):
    if byt < 64:
        return '0-' + str(byt) + '-'
    elif byt < 128:
        return '1-' + str(byt-64) + '-'
    elif byt < 192:
        return '2-' + str(byt-128) + '-'
    else:
        return '3-' + str(byt-192) + '-'

# To read the csv files and put them into a dataframe
def tables_b(file_path):
    try:
        col_names_b = ['F', 'X', 'Y', 'Description', 'Unit', 'Scale', 'Reference_Value', 'Data_width_bits']
        dfb = pd.read_csv(file_path, sep = ';', header = None , names = col_names_b)
    except Exception as e:
            print(e)
    return dfb

def tables_d(file_path):
    col_names_d = ['F', 'X', 'Y', 'dF', 'dX', 'dY']
    dfd = pd.read_csv(file_path, sep = ';', header = None, names = col_names_d, usecols = [0,1,2,3,4,5])
    return dfd

# To stock the dataframes tables into dictionary of descriptors

# Table B : descriptors with F = 0
def dico_descriptor_b(df0):
    dico_desc = {}
    try:
        for idx, row in df0.iterrows():
            dico_desc[str(row['F'])+"-"+str(row['X'])+"-"+str(row['Y'])] = {'Description' : row['Description'], 'Unit':row['Unit'], 'Scale': row['Scale'], 'Ref_Val':row['Reference_Value'], 'Data_width_bits':row['Data_width_bits']}
    except AttributeError as e: # corrected typo in original code: e.message -> AttributeError
            print(e)
    return dico_desc

# Table D : descriptors with F = 3 (each descriptor is a list of descriptors)
def dico_descriptor_d(df0):
    key1 = ''
    dico_desc = {}
    listed = []
    for i in range(df0.shape[0]):
        if not(pd.isna(df0.loc[i, 'F'])) and df0.loc[i, 'F'][-1] == '3':
            dico_desc[key1] = listed
            listed = []
            key1 = '3-' + str(int(df0.loc[i, 'X'])) + '-' + str(int(df0.loc[i, 'Y']))
            listed = [str(int(df0.loc[i, 'dF'])) + '-' + str(int(df0.loc[i, 'dX'])) + '-' + str(int(df0.loc[i, 'dY']))]
        elif not(pd.isna(df0.loc[i,'dF'])):
            listed += [str(int(df0.loc[i, 'dF'])) + '-' + str(int(df0.loc[i, 'dX'])) + '-' + str(int(df0.loc[i, 'dY']))]

    dico_desc[key1] = listed
    return dico_desc


class BufrDecoder:
    def __init__(self, dir_path_table, fic_tab_b, fic_tab_d, fic_local_tab_b, fic_local_tab_d, affiche_descriptors=True):
        self.dir_path_table = dir_path_table
        self.fic_tab_b = fic_tab_b
        self.fic_tab_d = fic_tab_d
        self.fic_local_tab_b = fic_local_tab_b
        self.fic_local_tab_d = fic_local_tab_d
        self.affiche_descriptors = affiche_descriptors
        self.dico_m_b = {}
        self.dico_m_d = {}
        self.dico_l_b = {}
        self.dico_l_d = {}

    def descri(self, desc):
        affiche = not self.fin_affichage and self.affiche_descriptors
        if desc in self.dico_l_b:
            r=self.dico_l_b[desc]
            if affiche:
                print(desc, ' : ', r)
            return r
        elif desc in self.dico_l_d:
            r=self.dico_l_d[desc]
            if affiche:
                print(desc, ' : ',r )
            return r

        elif desc in self.dico_m_b:
            r=self.dico_m_b[desc]
            if affiche:
                print(desc, ' : ', r)
            return r
        elif desc in self.dico_m_d:
            r=self.dico_m_d[desc]
            if affiche:
                print(desc, ' : ', r)
            return r
        else:
            r='UNKNOWN'
            if affiche:
                print(desc, ' UNKNOWN')
            return r

    def simple_desc(self, desc_elt, reader):
        descript_elt = self.descri(desc_elt)
        if type(descript_elt) is dict:
            if self.bit_new_width == 0:
                longueur = descript_elt['Data_width_bits'] + self.bit_width_plus
            else:
                longueur = self.bit_new_width
            description = descript_elt['Description']
            if not self.fin_affichage and self.affiche_descriptors:
                print('longueur : ', longueur, ' - Description : ', description)

            tot_bits = reader.readbits(longueur)

            # apply the reference value and the scale to the value of the data pointed by the descriptor
            if self.bit_ref_changed:
                if desc_elt in self.bit_new_ref:
                    val_data = (tot_bits + self.bit_new_ref[desc_elt])/10**(float(descript_elt['Scale'])+self.bit_scale_plus)
                else:
                    # just to avoid an error, set a default value to 0
                    val_data = 0
            else:
                val_data = (tot_bits + float(descript_elt['Ref_Val']) )/10**(float(descript_elt['Scale'])+self.bit_scale_plus)

            if descript_elt['Unit'] == 'CCITT IA5': #compte==0:
                try:
                    val_data = bits2bytes(bin(tot_bits))
                    print('  "', bits2bytes(bin(tot_bits)), '"')
                except:
                    pass
            elif not self.fin_affichage and self.affiche_descriptors:
                    print('  = ', val_data, descript_elt['Unit'])

            # stock the value in the list of values
            if description in self.datas_total:
                self.datas_total[description] += [val_data]
            else:
                self.datas_total[description] = [val_data]

            # stock the unit
            if not(description in self.datas_unites):
                self.datas_unites[description] = descript_elt['Unit']

            # if we want to print all the details
            if description == self.last_description:
                self.fin_affichage = True
            else:
                self.fin_affichage = False

            self.last_description = description

    def section1_v2(self, reader, bytes_size):
        x = reader.readbits(3*bytes_size)
        LENGTH_1 = x
        print('Length of section 1 : ', x)
        x = reader.readbits(1*bytes_size)
        BUFR_MASTER_TABLE = x
        print('Bufr master table : ', x)
        x = reader.readbits(1*bytes_size)
        SUB_CENTER_ID = x
        print('Identification of originating/generating sub-centre : ', x)
        x = reader.readbits(1*bytes_size)
        CENTER_ID = x
        print('Identification of originating/generating centre : ', x)
        x = reader.readbits(1*bytes_size)
        print('Update sequence number : ', x)
        x = reader.readbits(1*bytes_size)
        sect2 = x
        print('Optional (1) / No Optional (0) section follows : ', x)

        if sect2:
            print('yes, section 2 is present')
        else:
            print('no section 2 found')

        x = reader.readbits(1*bytes_size)
        print('Data Category (Table A) : ', x)
        x = reader.readbits(1*bytes_size)
        print('Data category sub-category : ', x)
        return LENGTH_1, BUFR_MASTER_TABLE, SUB_CENTER_ID, CENTER_ID, sect2

    def section1_v4(self, reader, bytes_size):
        x = reader.readbits(3*bytes_size)
        LENGTH_1 = x
        print('Length of section 1 : ', x)
        x = reader.readbits(1*bytes_size)
        BUFR_MASTER_TABLE = x
        print('Bufr master table : ', x)
        x = reader.readbits(2*bytes_size)
        CENTER_ID = x
        print('Identification of originating/generating centre : ', x)
        x = reader.readbits(2*bytes_size)
        SUB_CENTER_ID = x
        print('Identification of originating/generating sub-centre : ', x)
        x = reader.readbits(1*bytes_size)
        print('Update sequence number : ', x)
        x = reader.readbits(1*bytes_size)
        print('Optional (1) / No Optional (0) section follows : ', x)
        x = reader.readbits(1*bytes_size)
        print('Data Category (Table A) : ', x)
        x = reader.readbits(1*bytes_size)
        print('International data sub-category : ', x)
        x = reader.readbits(1*bytes_size)
        print('Local sub-category : ', x)
        return LENGTH_1, BUFR_MASTER_TABLE, SUB_CENTER_ID, CENTER_ID, False # sect2 is always false for v4?

    # additional datas at the end of section 1
    def section1end(self, version, LENGTH_1, reader, bytes_size):
        if version == 2:
            lim = 17
        elif version == 4:
            lim = 22
        if LENGTH_1> lim:
            print('SECTION 1 ending : ')
            for k in range(LENGTH_1 - lim):
                x = reader.readbits(1*bytes_size)
                print(x, ' ', chr(x))
            print('END OF SECTION 1')

    # optional section 2
    def section2(self, reader, bytes_size):
        x = reader.readbits(3*bytes_size)
        LENGTH_2 = x
        print('Length of section 2 : ', x)
        x = reader.readbits(1*bytes_size) # set to 0 (reserved)
        for k in range(LENGTH_2 - 4):
            x = reader.readbits(1*bytes_size)
            print(x, ' ', chr(x))

        print(' END OF SECTION 2')

    def descri_tableC(self, reader):
        new_ref = int(self.descriptors[self.index_descript].split('-')[2])
        if self.descriptors[self.index_descript].split('-')[1] == '1':
            # change data width
            if new_ref == 0:
                self.bit_width_plus = 0
            else:
                self.bit_width_plus = new_ref - 128
        elif self.descriptors[self.index_descript].split('-')[1] == '2':
            # change scale
            if new_ref == 0:
                self.bit_scale_plus = 0
            else:
                self.bit_scale_plus = new_ref - 128
        elif self.descriptors[self.index_descript].split('-')[1] == '3':
            # change reference value
            if 0 < new_ref:
                self.bit_ref_changed = True
                ybits = int(self.descriptors[self.index_descript].split('-')[2])
                self.index_descript += 1
                desc_new = self.descriptors[self.index_descript]
                while desc_new != '2-3-255':
                    result = reader.readbits(ybits)
                    if result >= 2**(ybits-1):
                        self.bit_new_ref[desc_new] = -1*(result - 2**(ybits-1))
                    else:
                        self.bit_new_ref[desc_new] = result
                    self.index_descript += 1
                    desc_new = self.descriptors[self.index_descript]

            else:
                self.bit_ref_changed = False
                self.bit_new_ref = {}

        elif self.descriptors[self.index_descript].split('-')[1] == '8':
            if new_ref == 0:
                self.bit_new_width = 0
            else:
                self.bit_new_width = 8*new_ref
        pass

    def decode_bufr_message(self, reader, bytes_size):
        self.datas_total = {}
        self.datas_unites = {}
        self.index_descript = 0
        self.compte = 0
        self.blocs_repetes  = 0
        self.nb_repetitions = 0
        self.bit_width_plus = 0
        self.bit_new_width = 0
        self.bit_scale_plus = 0
        self.bit_new_ref = {}
        self.bit_ref_changed = False
        self.last_description = ''
        self.fin_affichage = False
        self.descriptors = [] # Reset descriptors for each message
        sect2 = False

        # IDENTIFICATION SECTION
        try:
            x = reader.readbits(4*bytes_size)
        except:
            return None # Indicate end of file or error

        if not(str(bin(x))=="0b1000010010101010100011001010010"): # entete BUFR
            return None
        print(' ----------- BEGIN OF BUFR MESSAGE -----------')
        entete = bits2bytes(bin(x))
        print(entete)

        x = reader.readbits(3*bytes_size)
        print('Total length of Bufr message in bytes : ', x)
        x = reader.readbits(1*bytes_size)
        print('Bufr Edition number : ', x)

        # SECTION 1
        version = 0 # Initialize version
        if str(bin(x))=="0b10":
            version = 2
            LENGTH_1, BUFR_MASTER_TABLE, SUB_CENTER_ID, CENTER_ID, sect2 = self.section1_v2(reader, bytes_size)
        elif str(bin(x))=="0b100":
            version = 4
            LENGTH_1, BUFR_MASTER_TABLE, SUB_CENTER_ID, CENTER_ID, sect2 = self.section1_v4(reader, bytes_size)
        else:
            print('Version Inconnue')
            return None # Indicate error

        x = reader.readbits(1*bytes_size)
        MASTER_TABLE_NUMBER = x
        print('Version number of master table used : ', x)
        x = reader.readbits(1*bytes_size)
        LOCAL_TABLE_NUMBER = x
        print('Version number of local tables used : ', x)

        # LOAD USEFUL TABLES (Load tables at the beginning of each message decode in case of table changes)
        try:
            table_b = tables_b(os.path.join(self.dir_path_table, self.fic_tab_b.format(master=MASTER_TABLE_NUMBER)))
            self.dico_m_b = dico_descriptor_b(table_b)
        except:
            print(' ** UNABLE TO READ MASTER TABLE B ', MASTER_TABLE_NUMBER)
            self.dico_m_b = {}
        try:
            table_d = tables_d(os.path.join(self.dir_path_table, self.fic_tab_d.format(master=MASTER_TABLE_NUMBER)))
            self.dico_m_d = dico_descriptor_d(table_d)
        except:
            print(' ** UNABLE TO READ MASTER TABLE D', MASTER_TABLE_NUMBER)
            self.dico_m_d = {}
        try:
            local_table_b = tables_b(os.path.join(self.dir_path_table, self.fic_local_tab_b.format(center=CENTER_ID, local=LOCAL_TABLE_NUMBER)))
            self.dico_l_b = dico_descriptor_b(local_table_b)
        except:
            print(' ** UNABLE TO READ LOCAL TABLE B ' ,CENTER_ID ,"_" , LOCAL_TABLE_NUMBER)
            self.dico_l_b = {}
        try:
            local_table_d = tables_d(os.path.join(self.dir_path_table, self.fic_local_tab_d.format(center=CENTER_ID, local=LOCAL_TABLE_NUMBER)))
            self.dico_l_d = dico_descriptor_d(local_table_d)
        except:
            print(' ** UNABLE TO READ LOCAL TABLE D ' , CENTER_ID ,"_" , LOCAL_TABLE_NUMBER)
            self.dico_l_d = {}

        if version == 2:
            x = reader.readbits(1*bytes_size)
            print('Year : ', x)
        elif version == 4:
            x = reader.readbits(2*bytes_size)
            print('Year : ', x)

        x = reader.readbits(1*bytes_size)
        print('Month : ', x)
        x = reader.readbits(1*bytes_size)
        print('Day : ', x)
        x = reader.readbits(1*bytes_size)
        print('Hour : ', x)
        x = reader.readbits(1*bytes_size)
        print('Minute : ', x)
        if version == 4:
            x = reader.readbits(1*bytes_size)
            print('Second : ', x)

        # END OF SECTION 1
        self.section1end(version, LENGTH_1, reader, bytes_size)

        # OPTIONAL SECTION 2
        if sect2:
            self.section2(reader, bytes_size)

        # SECTION 3 ( Data Description )
        x = reader.readbits(3*bytes_size)
        LENGTH_3 = x
        print('Length of section 3 (Data Description) : ', x)
        x = reader.readbits(1*bytes_size) # SET TO 0 (reserved)
        x = reader.readbits(2*bytes_size)
        print('Number of data subsets : ', x)
        x = reader.readbits(1*bytes_size)
        #if version == 2:
        print('Observed/Compressed Data : ', x//128 , '/', (x//64)%2)


        desc = ''

        for i, k in enumerate(range(LENGTH_3 - 7)):
            x = reader.readbits(1*bytes_size)
            if i%2 == 1:
                desc += str(x)
                self.descriptors += [desc]
                desc = ''
            else:
                desc = bytes_desc(x)

        if self.affiche_descriptors:
            print('Descriptors :')
            print(self.descriptors)

        # SECTION 4 ( Datas )
        x = reader.readbits(3*bytes_size)
        LENGTH_4 = x
        print('Length of section 4 (Datas) : ', x)
        x = reader.readbits(1*bytes_size) # SET TO 0 (reserved)


        while True:
            if self.index_descript >= len(self.descriptors):
                print(' END OF DESCRIPTORS ')
                break
            if not self.fin_affichage and self.affiche_descriptors:
                print(self.descriptors[self.index_descript])

            if self.descriptors[self.index_descript][0] == '0':
                # F = 0 : single element descriptor (ref in Table B)
                self.simple_desc(self.descriptors[self.index_descript], reader)

            elif self.descriptors[self.index_descript][0] == '3':
                # F = 3 : list of descriptors (ref in table D)
                descript_elt = self.descri(self.descriptors[self.index_descript])
                for eltk in descript_elt:
                    if self.affiche_descriptors:
                        print(eltk)
                # insert the list of descriptors in place of the descriptor
                self.descriptors = self.descriptors[:self.index_descript] + descript_elt + self.descriptors[self.index_descript+1:]
                self.index_descript -= 1

            elif self.descriptors[self.index_descript][0] == '2':
                # F = 2 : Operator descriptor  (ref in table C)
                self.descri_tableC(reader)


            elif self.descriptors[self.index_descript][0] == '1':
                # Replication operator
                print('* REPETITIONS *')


                blocs_repetes = int(self.descriptors[self.index_descript].split('-')[1])
                try:
                    nbits_decal = self.descri(self.descriptors[self.index_descript+1])['Data_width_bits']
                except Exception as e:
                    print(e)
                    return None # Indicate error

                nb_repetitions = reader.readbits(nbits_decal)
                print('   number of descriptors replicated ', str(blocs_repetes), ' and number of replications = ', str(nb_repetitions))
                self.descriptors = self.descriptors[:self.index_descript] + self.descriptors[self.index_descript+2: self.index_descript+2+blocs_repetes]*nb_repetitions + self.descriptors[self.index_descript+2+blocs_repetes:]
                self.index_descript -= 1

            self.index_descript += 1
            self.compte += 1

        print(" ** END OF DATAS **")

        print('DATAS DESCRIPTORS NUMBER', len(self.datas_total))
        print('DATAS :')
        for key, value in self.datas_total.items():
            if len(value) < 10:
                # print values only for descriptors with few values
                print(' ', key, ' : ', value, ' (', self.datas_unites[key] if key in self.datas_unites else '', ')')
            else:
                # lot of values : print only the number of values
                print(' ', key, ' ( ',  len(value), ' data'+'s'*(len(value)>1) +')' )

        x = reader.readbits(4*bytes_size)
        try:
            print(' (7777 =) ', bits2bytes(bin(x))) #, 'END OF BUFR MESSAGE ', bufr_number)
        except:
            print('ERROR : end of file ?')
            return None

        print(' ----------- END OF BUFR MESSAGE -----------')
        return self.datas_total


    def decode(self, file_path, bytes_size=8):
        datas_messages = []
        bufr_number = 0
        with open(file_path, 'rb') as infile:
            with BitReader(infile) as reader:
                while True:
                    datas_total = self.decode_bufr_message(reader, bytes_size)
                    if datas_total is None: # No more messages or error
                        break
                    datas_messages.append(datas_total)
                    bufr_number += 1

        print(' END OF FILE ')
        nb = len(datas_messages)
        if nb > 0:
            print(' datas_messages contains ', nb, ' message'+'s'*(nb>1), 'in dictionary form, ', 'from 0 to'*(nb>1), str(nb-1)*(nb>1), 'datas_message[0]'*(nb==1))
        return datas_messages


# VARIABLES TO DECLARE
DIR_PATH = '/teamspace/studios/this_studio'
FILE_NAME = 'T_IMFR27_C_LFPW_20241228120000.bufr'
DIR_PATH_TABLE = '/teamspace/studios/this_studio/tables'
affiche_descriptors = True

FIC_TAB_B = 'bufrtabb_{master}.csv'
FIC_TAB_D = 'bufrtabd_{master}.csv'
FIC_LOCAL_TAB_B = 'localtabb_{center}_{local}.csv'
FIC_LOCAL_TAB_D = 'localtabd_{center}_{local}.csv'


# Main Program
decoder = BufrDecoder(DIR_PATH_TABLE, FIC_TAB_B, FIC_TAB_D, FIC_LOCAL_TAB_B, FIC_LOCAL_TAB_D, affiche_descriptors)
FILE_PATH = os.path.join(DIR_PATH, FILE_NAME)
datas_messages = decoder.decode(FILE_PATH)


if datas_messages and datas_messages[0]:
    # retrieve data from element and plot it
    rows = int(datas_messages[0]['Number of pixels per row'][0]) if 'Number of pixels per row' in datas_messages[0] else 512 # Default value if not found
    cols = int(datas_messages[0]['Number of pixels per column'][0]) if 'Number of pixels per column' in datas_messages[0] else 512 # Default value if not found
    data1 = np.reshape(datas_messages[0]['Horizontal reflectivity'], (rows, cols)) if 'Horizontal reflectivity' in datas_messages[0] else np.zeros((rows,cols)) # Default zeros if not found


    plt.imshow(data1)
    plt.savefig("reflect.png")
