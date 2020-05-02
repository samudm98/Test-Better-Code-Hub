package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion;

import co.gov.ugpp.parafiscales.servicios.liquidador.transversales.AppException;
import java.io.Serializable;

/**
 *
 * @author franzjr
 */
public interface LiquidadorFacade extends Serializable {

    public void liquidar(OpLiquidarSolTipo msjOpLiquidarSol)  throws AppException;

}
