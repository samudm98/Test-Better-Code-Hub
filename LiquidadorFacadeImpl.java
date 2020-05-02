package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion;

import co.gov.ugpp.parafiscales.servicios.liquidador.errortipo.v1.ErrorTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.util.ErrorEnum;
import co.gov.ugpp.parafiscales.servicios.liquidador.transversales.AppException;
import co.gov.ugpp.parafiscales.servicios.liquidador.util.ErrorUtil;
import java.util.ArrayList;
import java.util.List;
import javax.ejb.Stateless;
import javax.ejb.TransactionManagement;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author franzjr
 */
@Stateless
@TransactionManagement
public class LiquidadorFacadeImpl implements LiquidadorFacade {


    //private ExpedienteDao expedienteDao;
    
    //@EJB
    //private AportanteLIQDao aportanteDao;
    
    
    //@EJB
    //private PersonaDao personaDao;

    
    @Override
    public void liquidar(OpLiquidarSolTipo msjOpLiquidarSol) throws AppException {

        final List<ErrorTipo> errorTipoList = new ArrayList<>();

        if (msjOpLiquidarSol.getExpediente() == null
                || StringUtils.isBlank(msjOpLiquidarSol.getExpediente().getIdNumExpediente())
                || msjOpLiquidarSol.getIdentificacion() == null
                || StringUtils.isBlank(msjOpLiquidarSol.getIdentificacion().getCodTipoIdentificacion())
                || StringUtils.isBlank(msjOpLiquidarSol.getIdentificacion().getValNumeroIdentificacion())) 
        {

            
            //System.out.println("ERROR EXCEPTION opLiquidar: Los campos idExpediente, tipoIdentificacion, numero identificacion son obligatorios");
            
            errorTipoList.add(ErrorUtil.buildErrorTipo(ErrorEnum.ERROR_INTEGRIDAD_REFERENCIAL, "Los campos idExpediente, tipoIdentificacion, numero identificacion son obligatorios"));
            throw new AppException(errorTipoList);
        } 

    }

}
