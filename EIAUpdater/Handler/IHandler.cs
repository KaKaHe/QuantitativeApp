using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;
using EIAUpdater.Model;
using EIAUpdater.Database;

namespace EIAUpdater.Handler
{
    public interface IHandler
    {
        ILog Logger { get; set; }
        Configurations Config { get; set; }
    }
}
