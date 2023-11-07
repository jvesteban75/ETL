using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETL.Model
{
    public class Credenciales
    {
        public Credenciales(string userName, string password, string company)
        {


            this.UserName = userName;
            this.Password = password;
            this.CompanyDB = company;
        }
        public string UserName { get; set; }
        public string Password { get; set; }
        public string CompanyDB { get; set; }


    }
}
