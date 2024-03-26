package luci.sixsixsix.powerampache2.presentation.screens.songs

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.alpha
import androidx.hilt.navigation.compose.hiltViewModel
import com.google.accompanist.swiperefresh.SwipeRefresh
import com.google.accompanist.swiperefresh.rememberSwipeRefreshState
import com.ramcosta.composedestinations.annotation.Destination
import com.ramcosta.composedestinations.navigation.DestinationsNavigator
import luci.sixsixsix.powerampache2.presentation.common.LoadingScreen
import luci.sixsixsix.powerampache2.presentation.destinations.AlbumDetailScreenDestination
import luci.sixsixsix.powerampache2.presentation.destinations.ArtistDetailScreenDestination
import luci.sixsixsix.powerampache2.presentation.main.viewmodel.MainEvent
import luci.sixsixsix.powerampache2.presentation.main.viewmodel.MainViewModel
import luci.sixsixsix.powerampache2.presentation.common.SongItem
import luci.sixsixsix.powerampache2.presentation.common.SongItemEvent
import luci.sixsixsix.powerampache2.presentation.common.SubtitleString
import luci.sixsixsix.powerampache2.presentation.dialogs.AddToPlaylistOrQueueDialog
import luci.sixsixsix.powerampache2.presentation.dialogs.AddToPlaylistOrQueueDialogOpen
import luci.sixsixsix.powerampache2.presentation.dialogs.AddToPlaylistOrQueueDialogViewModel

@Composable
@Destination(start = false)
fun SongsListScreen(
    navigator: DestinationsNavigator,
    modifier: Modifier = Modifier,
    mainViewModel: MainViewModel,
    viewModel: SongsViewModel = hiltViewModel(),
    addToPlaylistOrQueueDialogViewModel: AddToPlaylistOrQueueDialogViewModel = hiltViewModel()
) {
    val swipeRefreshState = rememberSwipeRefreshState(isRefreshing = viewModel.state.isRefreshing)
    val state = viewModel.state
    var playlistsDialogOpen by remember { mutableStateOf(AddToPlaylistOrQueueDialogOpen(false)) }
    if (playlistsDialogOpen.isOpen) {
        if (playlistsDialogOpen.songs.isNotEmpty()) {
            AddToPlaylistOrQueueDialog(
                songs = playlistsDialogOpen.songs,
                onDismissRequest = {
                    playlistsDialogOpen = AddToPlaylistOrQueueDialogOpen(false)
                },
                mainViewModel = mainViewModel,
                viewModel = addToPlaylistOrQueueDialogViewModel,
                onCreatePlaylistRequest = {
                    playlistsDialogOpen = AddToPlaylistOrQueueDialogOpen(false)
                }
            )
        }
    }

    Box(modifier = modifier) {
        if (state.isLoading && state.songs.isEmpty()) {
            LoadingScreen()
        }
        Column {
            SwipeRefresh(
                state = swipeRefreshState,
                onRefresh = { viewModel.onEvent(SongsEvent.Refresh) }
            ) {
                LazyColumn(modifier = Modifier.fillMaxSize(),) {
                    items(
                        state.songs.size,
                        //key = { i -> state.songs[i].mediaId }
                    ) { i ->
                        val song = state.songs[i].song
                        val isOffline = state.songs[i].isOffline
                        SongItem(
                            song = song,
                            songItemEventListener = { event ->
                                when(event) {
                                    SongItemEvent.PLAY_NEXT ->
                                        mainViewModel.onEvent(MainEvent.OnAddSongToQueueNext(song))
                                    SongItemEvent.SHARE_SONG ->
                                        mainViewModel.onEvent(MainEvent.OnShareSong(song))
                                    SongItemEvent.DOWNLOAD_SONG ->
                                        mainViewModel.onEvent(MainEvent.OnDownloadSong(song))
                                    SongItemEvent.EXPORT_DOWNLOADED_SONG ->
                                        mainViewModel.onEvent(MainEvent.OnExportDownloadedSong(song))
                                    SongItemEvent.GO_TO_ALBUM -> navigator.navigate(
                                        AlbumDetailScreenDestination(albumId = song.album.id, album = null)
                                    )
                                    SongItemEvent.GO_TO_ARTIST -> navigator.navigate(
                                        ArtistDetailScreenDestination(artistId = song.artist.id, artist = null)
                                    )
                                    SongItemEvent.ADD_SONG_TO_QUEUE ->
                                        mainViewModel.onEvent(MainEvent.OnAddSongToQueue(song))
                                    SongItemEvent.ADD_SONG_TO_PLAYLIST ->
                                        playlistsDialogOpen = AddToPlaylistOrQueueDialogOpen(true, listOf(song))
                                }
                            },
                            subtitleString = SubtitleString.ARTIST,
                            isSongDownloaded = isOffline,
                            modifier = Modifier
                                .fillMaxWidth()
                                .clickable {
                                    mainViewModel.onEvent(MainEvent.PlaySongAddToQueueTop(song, state.getSongList()))
//                                    viewModel.onEvent(SongsEvent.OnSongSelected(song))
//                                    mainViewModel.onEvent(MainEvent.Play(song))
                                }
                        )
                        // TODO decide to include or not this
                        // footer(i = i, state = state)
                    }
                }
            }
        }
    }
}

@Composable
fun footer(i: Int, state: SongsState) {
    if (i < state.songs.size - 1) {
        // if not last item add a divider
        // TODO: do I want a divider? Divider(modifier = Modifier.padding(horizontal = 16.dp))
    } else if (i == state.songs.size - 1) {
        // TODO should this screen be allowed to load more ?
        Column(modifier = Modifier.fillMaxWidth()) {
            CircularProgressIndicator(
                modifier = Modifier
                    .align(Alignment.CenterHorizontally)
                    .alpha(
                        if (state.isFetchingMore) {
                            1.0f
                        } else {
                            0.0f
                        }
                    )
            )
        }
    }
}
