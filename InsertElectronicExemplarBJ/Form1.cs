using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace InsertElectronicExemplarBJ
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            int PIN = 0;
            try
            {
                PIN = int.Parse(textBox1.Text);
            }
            catch
            {
                MessageBox.Show("ПИН должен быть числом!");
                return;
            }


            ElectronicExemplarInserter ec = new ElectronicExemplarInserter(PIN, comboBox1.Text);
            ec.InsertElectronicExemplar(ElectronicExemplarType.Free);



        }

        private void Form1_Load(object sender, EventArgs e)
        {
            comboBox1.SelectedIndex = 0;
        }



    }
}
