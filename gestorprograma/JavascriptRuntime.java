package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class JavascriptRuntime implements RuntimeScript
{
   private ScriptEngineManager scriptEngineManager;

   private ScriptEngine scriptEngine;

   public JavascriptRuntime()
   {
      scriptEngineManager = new ScriptEngineManager();
      scriptEngine = scriptEngineManager.getEngineByName("JavaScript");
   }

   @Override
   public Double ejecutarScript(String script) throws Exception
   {
      Object convertedValue = scriptEngine.eval(script);
      return (Double) convertedValue;
   }

}
