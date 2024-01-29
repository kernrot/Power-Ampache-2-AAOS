package luci.sixsixsix.powerampache2.presentation.main

import android.app.Application
import android.widget.Toast
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.SavedStateHandle
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.SavedStateHandleSaveableApi
import androidx.lifecycle.viewmodel.compose.saveable
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.launch
import luci.sixsixsix.mrlog.L
import luci.sixsixsix.powerampache2.common.Resource
import luci.sixsixsix.powerampache2.common.sha256
import luci.sixsixsix.powerampache2.data.local.MusicDatabase
import luci.sixsixsix.powerampache2.data.local.entities.toSession
import luci.sixsixsix.powerampache2.data.remote.PingScheduler
import luci.sixsixsix.powerampache2.domain.MusicRepository
import luci.sixsixsix.powerampache2.domain.utils.AlarmScheduler
import luci.sixsixsix.powerampache2.player.MusicPlaylistManager
import luci.sixsixsix.powerampache2.presentation.screens.home.HomeScreenState
import javax.inject.Inject

@HiltViewModel
@OptIn(SavedStateHandleSaveableApi::class)
class AuthViewModel @Inject constructor(
    private val repository: MusicRepository,
    private val playlistManager: MusicPlaylistManager,
    pingScheduler: AlarmScheduler,
    application: Application,
    private val savedStateHandle: SavedStateHandle
) : AndroidViewModel(application) {
    //var stateSaved = savedStateHandle.getStateFlow("keyauth", AuthState())

    // var state by mutableStateOf(AuthState())
    var state by savedStateHandle.saveable { mutableStateOf(AuthState()) }

//    var state = savedStateHandle.get<AuthState>("keyauth") ?: AuthState()
//        set(value) {
//            savedStateHandle["keyauth"] = value
//            field = value
//        }

    init {
        observeMessages()
        state = state.copy(isLoading = true)
        verifyAndAutologin()
        // Listen to changes of the session table from the database
        repository.sessionLiveData.observeForever {
            state = state.copy(session = it)
            L(it)
            if (it == null) {
                pingScheduler.cancel()
                playlistManager.reset()
                // apply default settings

                // setting the session to null will show the login screen, but the autologin call
                // will immediately set isLoading to true which will show the loading screen instead
                state = state.copy(session = null, isLoading = true)
                // autologin will log back in if credentials are correct
                viewModelScope.launch {
                    autologin()
                }
            } else {
                pingScheduler.schedule()
            }
        }

        viewModelScope.launch {
            repository.userLiveData.observeForever {
                L(it)
                it?.let { user ->
                    state = state.copy(user = user)
                }
            }
        }
    }

    private fun observeMessages() {
        viewModelScope.launch {
            playlistManager.errorMessageState.collect { errorState ->
                errorState.errorMessage?.let {
                    state = state.copy(error = it)
                }
                L(errorState.errorMessage)
            }
        }
    }

    fun verifyAndAutologin() {
        viewModelScope.launch {
            // try to login with saved auth token
            when (val ping = repository.ping()) {
                is Resource.Success -> {
                    //   If the session returned by ping is null, the token is probably expired and
                    // the user is no longer authorized
                    //   If the session is not null we are authorized and the auth token in the
                    // session object is refreshed
                    ping.data?.second?.let {
                        state = state.copy(session = it, isLoading = false)
                    } ?: run {
                        playlistManager.reset()
                        // do not show loading screen during ping, only during autologin
                        state = state.copy(isLoading = true)
                        autologin()
                    }
                }

                is Resource.Error -> {
                    state = state.copy(isLoading = false)
                }

                is Resource.Loading ->
                    if (!ping.isLoading) { state = state.copy(isLoading = false) }
            }
        }
    }

    private suspend fun autologin() {
        repository
            .autoLogin()
            .collect { result ->
                when (result) {
                    is Resource.Success -> {
                        result.data?.let { auth ->
                            L("AuthViewModel", auth)
                            state = state.copy(session = auth)
                        }
                    }

                    is Resource.Error -> state =
                        state.copy(isLoading = false)

                    is Resource.Loading -> state = state.copy(isLoading = result.isLoading)
                }
            }
    }

    fun onEvent(event: AuthEvent) {
        when (event) {
            is AuthEvent.Login -> login()
            is AuthEvent.TryAutoLogin -> {}
            is AuthEvent.OnChangePassword -> state = state.copy(password = event.password)
            is AuthEvent.OnChangeServerUrl -> state = state.copy(url = event.url)
            is AuthEvent.OnChangeUsername -> state = state.copy(username = event.username)
            is AuthEvent.OnChangeAuthToken -> state = state.copy(authToken = event.token)
            AuthEvent.SignUp -> Toast.makeText(getApplication(), "Coming Soon", Toast.LENGTH_LONG).show()
        }
    }

    private fun login(
        username: String = state.username,
        password: String = state.password,
        serverUrl: String = state.url,
        authToken: String = state.authToken
    ) {
        viewModelScope.launch {
            repository.authorize(username, password.sha256(), serverUrl, authToken)
                .collect { result ->
                    when (result) {
                        is Resource.Success -> {
                            result.data?.let { auth ->
                                state = state.copy(session = auth)
                                playlistManager.updateErrorMessage("")
                            }
                        }

                        is Resource.Error -> {
                            state = state.copy(
                                isLoading = false
                            )
                        }

                        is Resource.Loading -> state = state.copy(isLoading = result.isLoading)
                    }
                }
        }
    }
}
